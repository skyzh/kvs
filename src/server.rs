use crate::{CommandRequest, CommandResponse, KvStoreError, KvsEngine, Result};
use slog::{info, Logger};
use std::io::{BufRead, BufReader, BufWriter};
use std::net::TcpListener;

pub struct KvsServer {
    listener: TcpListener,
    kvs_engine: Box<dyn KvsEngine>,
}

impl KvsServer {
    pub fn new(listener: TcpListener, kvs_engine: Box<dyn KvsEngine>) -> Self {
        Self {
            listener,
            kvs_engine,
        }
    }
    pub fn serve(&mut self, log: &Logger) -> Result<()> {
        for connection in self.listener.incoming() {
            let mut connection = connection?;
            info!(log, "new connection"; "peer" => connection.peer_addr()?);
            let mut reader = BufReader::new(&mut connection);
            let mut line = String::new();
            reader.read_line(&mut line)?;
            drop(reader);
            let response = match serde_json::from_str(line.as_str())? {
                CommandRequest::Get { key } => {
                    info!(log, "client"; "command" => "get" ,"key" => &key);
                    match self.kvs_engine.get(key) {
                        Ok(value) => CommandResponse::Value { value },
                        Err(e) => CommandResponse::Error {
                            reason: format!("{:?}", e),
                        },
                    }
                }
                CommandRequest::Set { key, value } => {
                    info!(log, "client"; "command" => "set", "key" => &key, "value" => &value);
                    match self.kvs_engine.set(key, value) {
                        Ok(_) => CommandResponse::Success {},
                        Err(e) => CommandResponse::Error {
                            reason: format!("{:?}", e),
                        },
                    }
                }
                CommandRequest::Remove { key } => {
                    info!(log, "client"; "command" => "rm", "key" => &key);
                    match self.kvs_engine.remove(key) {
                        Ok(_) => CommandResponse::Success {},
                        Err(e) => {
                            if let KvStoreError::KeyNotFound { .. } = e {
                                CommandResponse::KeyNotFound {}
                            } else {
                                CommandResponse::Error {
                                    reason: format!("{:?}", e),
                                }
                            }
                        }
                    }
                }
            };
            let mut writer = BufWriter::new(connection);
            serde_json::to_writer(&mut writer, &response)?;
        }

        Ok(())
    }
}
