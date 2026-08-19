use std::{collections::HashMap, io::{Read, Write}, net::TcpStream, ptr, slice, sync::{atomic::{AtomicI32, Ordering}, Mutex, OnceLock}, thread, time::Duration};

struct Connection {
    stream: TcpStream,
    rx: Vec<u8>,
}

static CONNECTIONS: OnceLock<Mutex<HashMap<i32, Connection>>> = OnceLock::new();
static NEXT_HANDLE: AtomicI32 = AtomicI32::new(1);

fn connections() -> &'static Mutex<HashMap<i32, Connection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[no_mangle]
pub unsafe extern "system" fn CopierRConnect(endpoint: *const u16) -> i32 {
    let endpoint = match utf16_ptr_to_string(endpoint) {
        Some(value) if !value.is_empty() => value,
        _ => return -1,
    };
    let stream = match TcpStream::connect(endpoint) {
        Ok(stream) => stream,
        Err(_) => return -2,
    };
    if stream.set_nodelay(true).is_err() || stream.set_nonblocking(true).is_err() {
        return -3;
    }
    let handle = NEXT_HANDLE.fetch_add(1, Ordering::Relaxed).max(1);
    match connections().lock() {
        Ok(mut map) => {
            map.insert(handle, Connection { stream, rx: Vec::with_capacity(4096) });
            handle
        }
        Err(_) => -4,
    }
}

#[no_mangle]
pub unsafe extern "system" fn CopierRSend(handle: i32, line: *const u16) -> i32 {
    let mut payload = match utf16_ptr_to_string(line) {
        Some(value) => value.into_bytes(),
        None => return -1,
    };
    if !payload.ends_with(b"\n") {
        payload.push(b'\n');
    }
    let mut map = match connections().lock() {
        Ok(map) => map,
        Err(_) => return -2,
    };
    let Some(connection) = map.get_mut(&handle) else { return -3; };
    match write_nonblocking(&mut connection.stream, &payload) {
        Ok(()) => payload.len() as i32,
        Err(_) => -4,
    }
}

#[no_mangle]
pub unsafe extern "system" fn CopierRPoll(handle: i32, buffer: *mut u8, capacity: i32) -> i32 {
    if buffer.is_null() || capacity <= 0 {
        return -1;
    }
    let mut map = match connections().lock() {
        Ok(map) => map,
        Err(_) => return -2,
    };
    let Some(connection) = map.get_mut(&handle) else { return -3; };

    let mut scratch = [0u8; 4096];
    loop {
        match connection.stream.read(&mut scratch) {
            Ok(0) => return -5,
            Ok(read) => connection.rx.extend_from_slice(&scratch[..read]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(_) => return -6,
        }
    }

    let Some(newline) = connection.rx.iter().position(|byte| *byte == b'\n') else {
        return 0;
    };
    let line_len = newline + 1;
    if line_len > capacity as usize {
        return -7;
    }
    ptr::copy_nonoverlapping(connection.rx.as_ptr(), buffer, line_len);
    connection.rx.drain(..line_len);
    line_len as i32
}

#[no_mangle]
pub extern "system" fn CopierRClose(handle: i32) {
    if let Ok(mut map) = connections().lock() {
        map.remove(&handle);
    }
}

fn write_nonblocking(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let mut written = 0usize;
    let mut spins = 0usize;
    while written < payload.len() {
        match stream.write(&payload[written..]) {
            Ok(0) => return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "socket write returned zero")),
            Ok(count) => {
                written += count;
                spins = 0;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                spins += 1;
                if spins < 32 {
                    thread::yield_now();
                } else if spins < 256 {
                    thread::sleep(Duration::from_micros(50));
                } else {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

unsafe fn utf16_ptr_to_string(ptr: *const u16) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let mut len = 0usize;
    while len < 32_768 && *ptr.add(len) != 0 {
        len += 1;
    }
    if len == 32_768 {
        return None;
    }
    Some(String::from_utf16_lossy(slice::from_raw_parts(ptr, len)))
}
