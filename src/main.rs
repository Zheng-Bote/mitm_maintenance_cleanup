use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, Pool, Postgres};
use std::env;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::timeout;

const APP_NAME: &str = "MitM Cleanup Job";
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FsCleanupRule {
    directory_path: String,
    file_pattern: String,
    retention_days: i32,
}

#[derive(Serialize, Deserialize, Debug, Default)]
struct CleanupArgs {
    #[serde(default = "default_target_fragments")]
    target_fragments_retention_days: i32,
    #[serde(default = "default_raw_ingestion")]
    raw_ingestion_orphan_days: i32,
    #[serde(default = "default_admin_audit")]
    admin_audit_logs_retention_days: i32,
    #[serde(default = "default_job_audit")]
    job_audit_logs_retention_days: i32,
    #[serde(default = "default_system_logs")]
    system_logs_retention_days: i32,
    #[serde(default = "default_job_status")]
    job_status_events_retention_days: i32,
    #[serde(default = "default_transformation_errors")]
    transformation_errors_retention_days: i32,
    #[serde(default = "default_program_runs")]
    program_runs_retention_days: i32,
    #[serde(default = "default_packages")]
    packages_retention_days: i32,
    #[serde(default = "default_dlq_resolved")]
    dlq_resolved_retention_days: i32,
    #[serde(default = "default_fs_rules")]
    fs_cleanup_rules: Vec<FsCleanupRule>,
    #[serde(default = "default_timeout")]
    timeout_minutes: i32,
}

fn default_target_fragments() -> i32 { 7 }
fn default_raw_ingestion() -> i32 { 14 }
fn default_admin_audit() -> i32 { 90 }
fn default_job_audit() -> i32 { 30 }
fn default_system_logs() -> i32 { 30 }
fn default_job_status() -> i32 { 14 }
fn default_transformation_errors() -> i32 { 30 }
fn default_program_runs() -> i32 { 30 }
fn default_packages() -> i32 { 30 }
fn default_dlq_resolved() -> i32 { 90 }
fn default_fs_rules() -> Vec<FsCleanupRule> { vec![] }
fn default_timeout() -> i32 { 60 }

#[derive(Serialize)]
struct StatusEvent<'a> {
    run_id: i32,
    r#type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    component: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<&'a str>,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<i32>,
}

#[derive(Clone)]
struct IpcClient {
    socket_path: String,
    run_id: i32,
    component: String,
}

impl IpcClient {
    async fn send_event(&self, status: &str, message: &str, progress: i32) {
        if self.socket_path.is_empty() {
            return;
        }
        let event = StatusEvent {
            run_id: self.run_id,
            r#type: "status",
            component: Some(self.component.as_str()),
            status: Some(status),
            message: message.to_string(),
            progress: Some(progress),
        };
        let mut data = serde_json::to_vec(&event).unwrap_or_default();
        data.push(b'\n');

        if let Ok(mut stream) = UnixStream::connect(&self.socket_path).await {
            let _ = stream.write_all(&data).await;
        } else {
            eprintln!("[IPC ERROR] Failed to connect to scheduler socket");
        }
    }

    async fn send_audit(&self, message: &str) {
        if self.socket_path.is_empty() {
            return;
        }
        let event = StatusEvent {
            run_id: self.run_id,
            r#type: "audit",
            component: Some(&self.component),
            status: None,
            message: message.to_string(),
            progress: None,
        };
        let mut data = serde_json::to_vec(&event).unwrap_or_default();
        data.push(b'\n');

        if let Ok(mut stream) = UnixStream::connect(&self.socket_path).await {
            let _ = stream.write_all(&data).await;
        } else {
            eprintln!("[IPC ERROR] Failed to connect to scheduler socket");
        }
    }
}

async fn fetch_credentials_from_scheduler(
    run_id: i32,
    socket_path: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let mut stream = timeout(
        Duration::from_secs(5),
        UnixStream::connect(socket_path),
    )
    .await??;

    let req = serde_json::json!({
        "type": "get_credentials",
        "run_id": run_id
    });
    let mut data = serde_json::to_vec(&req)?;
    data.push(b'\n');
    stream.write_all(&data).await?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let resp: Value = serde_json::from_str(&line)?;
    let db_config_json = resp["db_config_json"].as_str().unwrap_or("").to_string();
    let master_key = resp["master_key"].as_str().unwrap_or("").to_string();

    Ok((db_config_json, master_key))
}

async fn delete_in_batches(
    pool: &Pool<Postgres>,
    table_name: &str,
    condition: &str,
    arg: i32,
) -> Result<i32, Box<dyn std::error::Error>> {
    let mut total_deleted = 0;
    let batch_size = 10000;

    let query = format!(
        "DELETE FROM {table_name} WHERE ctid IN (SELECT ctid FROM {table_name} WHERE {condition} LIMIT {batch_size})"
    );

    loop {
        // We bind the integer parameter (converted to f64 just in case postgres requires double for interval mult)
        let rows_affected = sqlx::query(&query)
            .bind(arg as f64)
            .execute(pool)
            .await?
            .rows_affected();
        
        total_deleted += rows_affected as i32;
        if rows_affected == 0 {
            break;
        }
    }

    let vacuum_query = format!("VACUUM ANALYZE {}", table_name);
    if let Err(e) = pool.execute(vacuum_query.as_str()).await {
        eprintln!("Warning: VACUUM ANALYZE failed on {}: {}", table_name, e);
    }

    Ok(total_deleted)
}

#[derive(Deserialize)]
struct DbConfigInner {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    #[serde(default)]
    sslmode: bool,
}
#[derive(Deserialize)]
struct FullDbConfig {
    db: DbConfigInner,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_id_str = env::var("RUN_ID").unwrap_or_default();
    let socket_path = env::var("SCHEDULER_SOCKET_PATH").unwrap_or_default();

    let mut ipc_client = None;
    if let Ok(run_id) = run_id_str.parse::<i32>() {
        if !socket_path.is_empty() {
            ipc_client = Some(IpcClient {
                socket_path: socket_path.clone(),
                run_id,
                component: "mitm_cleanup".to_string(),
            });

            if let Ok((db_cfg, master_key)) = fetch_credentials_from_scheduler(run_id, &socket_path).await {
                if !db_cfg.is_empty() {
                    unsafe { env::set_var("MITM_DB_CONFIG_JSON", db_cfg); }
                }
                if !master_key.is_empty() {
                    unsafe { env::set_var("MASTER_KEY", master_key); }
                }
            } else {
                eprintln!("[IPC Warning] Failed to get credentials from scheduler");
            }
        }
    }

    let version = VERSION.split('-').next().unwrap_or(VERSION);

    if let Some(ipc) = &ipc_client {
        ipc.send_event("started", &format!("{} ({}) started", APP_NAME, version), 0).await;
        ipc.send_audit(&format!("{} ({}) started", APP_NAME, version)).await;
    }

    let json_config = env::var("MITM_DB_CONFIG_JSON").unwrap_or_default();
    let mut config_source = "Environment Variables";
    
    let mitm_dsn = if !json_config.is_empty() {
        let full_cfg: FullDbConfig = match serde_json::from_str(&json_config) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to parse MitM JSON configuration: {}", e);
                if let Some(ipc) = &ipc_client { ipc.send_event("failed", &format!("Parse error: {}", e), 0).await; }
                std::process::exit(1);
            }
        };
        config_source = "JSON Config (MITM_DB_CONFIG_JSON)";
        let ssl = if full_cfg.db.sslmode { "require" } else { "disable" };
        unsafe { env::set_var("MITM_DB_SSLMODE", ssl); }
        format!(
            "postgres://{}:{}@{}:{}/{}?sslmode={}",
            full_cfg.db.user, full_cfg.db.password, full_cfg.db.host, full_cfg.db.port, full_cfg.db.database, ssl
        )
    } else {
        let host = env::var("MITM_DB_HOST").unwrap_or_default();
        if host.is_empty() {
            if let Some(ipc) = &ipc_client {
                ipc.send_event("failed", "MitM database configuration missing in ENV", 0).await;
            }
            eprintln!("MitM database credentials not found");
            std::process::exit(1);
        }
        let port = env::var("MITM_DB_PORT").unwrap_or_else(|_| "5432".to_string());
        let user = env::var("MITM_DB_USER").unwrap_or_default();
        let pass = env::var("MITM_DB_PASSWORD").unwrap_or_default();
        let db = env::var("MITM_DB_NAME").unwrap_or_default();
        let ssl = env::var("MITM_DB_SSLMODE").unwrap_or_else(|_| "disable".to_string());
        
        let ssl_val = match ssl.as_str() {
            "true" => "require",
            "false" => "disable",
            _ => ssl.as_str(),
        };

        format!("postgres://{}:{}@{}:{}/{}?sslmode={}", user, pass, host, port, db, ssl_val)
    };


    if let Some(ipc) = &ipc_client {
        ipc.send_audit(&format!("Loaded database configuration from {}", config_source)).await;
    }

    let mut args = CleanupArgs::default();
    if let Some(arg_str) = std::env::args().nth(1) {
        if let Ok(parsed) = serde_json::from_str::<CleanupArgs>(&arg_str) {
            args = parsed;
        } else {
            eprintln!("Warning: Failed to parse collector arguments from os.Args[1]. Using defaults.");
        }
    }

    let pool = match PgPoolOptions::new()
        .max_connections(3)
        .idle_timeout(Duration::from_secs(5 * 60))
        .max_lifetime(Duration::from_secs(60 * 60))
        .connect(&mitm_dsn)
        .await {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Failed to connect: {}", e);
                if let Some(ipc) = &ipc_client { ipc.send_event("failed", &format!("DB connect error: {}", e), 0).await; }
                std::process::exit(1);
            }
        };

    if let Some(ipc) = &ipc_client {
        ipc.send_event("processing", "Connected to MitM database. Starting cleanup...", 10).await;
    }
    log_system(&pool, "INFO", &format!("{} ({}) started", APP_NAME, version)).await;


    let mut total_deleted = 0;
    let mut errors_occurred = false;

    // 1. Clean Target Fragments
    match delete_in_batches(&pool, "target_fragments", "LOWER(delivery_status) = 'delivered' AND created_at < NOW() - INTERVAL '1 day' * $1", args.target_fragments_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} delivered target fragments older than {} days.", count, args.target_fragments_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning target_fragments: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning target_fragments: {}", e)).await;
            errors_occurred = true;
        }
    }
    
    if let Some(ipc) = &ipc_client {
        ipc.send_event("processing", "Cleaned target fragments.", 30).await;
    }

    // 2. Clean Orphaned Raw Ingestion
    match delete_in_batches(&pool, "raw_ingestion", "status IN ('pending', 'processed', 'failed_validation') AND created_at < NOW() - INTERVAL '1 day' * $1", args.raw_ingestion_orphan_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} raw ingestion fragments older than {} days.", count, args.raw_ingestion_orphan_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning raw_ingestion: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning raw_ingestion: {}", e)).await;
            errors_occurred = true;
        }
    }

    if let Some(ipc) = &ipc_client {
        ipc.send_event("processing", "Cleaned raw fragments.", 50).await;
    }

    // 3. Clean Audit Logs
    match delete_in_batches(&pool, "job_audit_logs", "ts < NOW() - INTERVAL '1 day' * $1", args.job_audit_logs_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} job audit logs older than {} days.", count, args.job_audit_logs_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning job_audit_logs: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning job_audit_logs: {}", e)).await;
            errors_occurred = true;
        }
    }
    match delete_in_batches(&pool, "admin_audit_logs", "ts < NOW() - INTERVAL '1 day' * $1", args.admin_audit_logs_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} admin audit logs older than {} days.", count, args.admin_audit_logs_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning admin_audit_logs: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning admin_audit_logs: {}", e)).await;
            errors_occurred = true;
        }
    }

    if let Some(ipc) = &ipc_client {
        ipc.send_event("processing", "Cleaned audit logs.", 60).await;
    }

    // 4. Clean System Logs
    match delete_in_batches(&pool, "system_logs", "ts < NOW() - INTERVAL '1 day' * $1", args.system_logs_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} system logs older than {} days.", count, args.system_logs_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning system_logs: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning system_logs: {}", e)).await;
            errors_occurred = true;
        }
    }

    // 5. Clean Job Status Events
    match delete_in_batches(&pool, "job_status_events", "ts < NOW() - INTERVAL '1 day' * $1", args.job_status_events_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} job status events older than {} days.", count, args.job_status_events_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning job_status_events: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning job_status_events: {}", e)).await;
            errors_occurred = true;
        }
    }

    // 6. Clean Transformation Errors
    match delete_in_batches(&pool, "transformation_errors", "created_at < NOW() - INTERVAL '1 day' * $1", args.transformation_errors_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} transformation errors older than {} days.", count, args.transformation_errors_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning transformation_errors: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning transformation_errors: {}", e)).await;
            errors_occurred = true;
        }
    }

    // 7. Clean Program Runs
    match delete_in_batches(&pool, "program_runs", "started_at < NOW() - INTERVAL '1 day' * $1", args.program_runs_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} program runs older than {} days.", count, args.program_runs_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning program_runs: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning program_runs: {}", e)).await;
            errors_occurred = true;
        }
    }

    // 8. Clean Packages
    match delete_in_batches(&pool, "packages", "LOWER(status) = 'delivered' AND created_at < NOW() - INTERVAL '1 day' * $1", args.packages_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} delivered packages older than {} days.", count, args.packages_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning packages: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning packages: {}", e)).await;
            errors_occurred = true;
        }
    }

    // 9. Clean Dead Letter Queue
    match delete_in_batches(&pool, "dead_letter_queue", "resolved = TRUE AND failed_at < NOW() - INTERVAL '1 day' * $1", args.dlq_resolved_retention_days).await {
        Ok(count) => {
            total_deleted += count;
            if let Some(ipc) = &ipc_client {
                ipc.send_audit(&format!("Deleted {} resolved DLQ records older than {} days.", count, args.dlq_resolved_retention_days)).await;
            }
        },
        Err(e) => {
            eprintln!("Error cleaning dead_letter_queue: {}", e); log_system(&pool, "ERROR", &format!("Error cleaning dead_letter_queue: {}", e)).await;
            errors_occurred = true;
        }
    }

    // 10. Clean Filesystem
    let mut fs_deleted = 0;
    if !args.fs_cleanup_rules.is_empty() {
        match clean_filesystem(&args.fs_cleanup_rules) {
            Ok(count) => {
                fs_deleted = count;
                if let Some(ipc) = &ipc_client {
                    ipc.send_audit(&format!("Deleted {} outdated files from filesystem.", count)).await;
                }
            },
            Err(e) => {
                eprintln!("Error cleaning filesystem: {}", e);
                log_system(&pool, "ERROR", &format!("Error cleaning filesystem: {}", e)).await;
                errors_occurred = true;
            }
        }
    }

    if let Some(ipc) = &ipc_client {
        ipc.send_audit(&format!("{} ({}) finished", APP_NAME, version)).await;
        
        if errors_occurred {
            let msg = format!("Cleanup partially failed. Removed {} DB records and {} files, but some errors occurred.", total_deleted, fs_deleted);
            ipc.send_event("failed", &msg, 100).await;
            println!("{}", msg);
        } else {
            let msg = format!("Cleanup complete. Removed {} DB records and {} files.", total_deleted, fs_deleted);
            ipc.send_event("finished", &msg, 100).await;
            println!("{}", msg);
        }
    } else {
        if errors_occurred {
            println!("Cleanup partially failed. Removed {} DB records and {} files, but some errors occurred.", total_deleted, fs_deleted);
        } else {
            println!("Cleanup complete. Removed {} DB records and {} files.", total_deleted, fs_deleted);
        }
    }
    
    log_system(&pool, "INFO", &format!("{} ({}) finished", APP_NAME, version)).await;

    Ok(())
}
async fn log_system(pool: &Pool<Postgres>, level: &str, message: &str) {
    let _ = sqlx::query("INSERT INTO system_logs (level, component, message) VALUES ($1, 'mitm_cleanup', $2)")
        .bind(level)
        .bind(message)
        .execute(pool)
        .await;
}

fn clean_filesystem(rules: &[FsCleanupRule]) -> Result<i32, Box<dyn std::error::Error>> {
    let mut total_deleted = 0;
    let now = std::time::SystemTime::now();

    for rule in rules {
        let pattern = format!("{}/{}", rule.directory_path.trim_end_matches('/'), rule.file_pattern);
        
        for entry in glob::glob(&pattern)? {
            if let Ok(path) = entry {
                if path.is_file() {
                    if let Ok(metadata) = std::fs::metadata(&path) {
                        if let Ok(modified) = metadata.modified() {
                            if let Ok(age) = now.duration_since(modified) {
                                if age.as_secs() > (rule.retention_days as u64 * 24 * 60 * 60) {
                                    if let Err(e) = std::fs::remove_file(&path) {
                                        eprintln!("Failed to delete {}: {}", path.display(), e);
                                    } else {
                                        total_deleted += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    Ok(total_deleted)
}
