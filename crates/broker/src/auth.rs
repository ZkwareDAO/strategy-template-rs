//! WS 握手认证原语 —— 对齐 trading_service_rs/docs/服务端与客户端连接.md
//!
//! signature = hex(HMAC_SHA256(key = SECRET_KEY, strategy_name + nonce))

use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// 生成一次性 nonce：32 字节随机数的 hex（64 字符）。
pub fn new_nonce() -> String {
    let mut buf = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

/// HMAC-SHA256(secret, strategy_name + nonce) 的 hex。
pub fn sign(secret: &str, strategy_name: &str, nonce: &str) -> String {
    let mut mac =
        <HmacSha256 as Mac>::new_from_slice(secret.as_bytes()).expect("HMAC 接受任意长度 key");
    mac.update(strategy_name.as_bytes());
    mac.update(nonce.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// 恒定时间验签；签名长度不符/不匹配均返回 false。
pub fn verify(secret: &str, strategy_name: &str, nonce: &str, signature: &str) -> bool {
    use subtle::ConstantTimeEq;
    let expected = sign(secret, strategy_name, nonce);
    expected.as_bytes().ct_eq(signature.as_bytes()).into()
}

/// RFC 3986 unreserved 字符不编码，其余 %XX（与 trading-service strategy-mock 一致）。
pub fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// 拼握手 URL：`base?strategy_name=&nonce=&signature=`（base 已含 query 时用 &）。
pub fn connect_url(base: &str, strategy_name: &str, nonce: &str, signature: &str) -> String {
    let sep = if base.contains('?') { '&' } else { '?' };
    format!(
        "{base}{sep}strategy_name={}&nonce={nonce}&signature={signature}",
        percent_encode(strategy_name)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// HMAC-SHA256 公开测试向量：key/msg 均为空串。
    #[test]
    fn sign_matches_known_vector() {
        assert_eq!(
            sign("", "", ""),
            "b613679a0814d9ec772f95d778c35fc5ff1697c493715653c6c712144292c5ad"
        );
    }

    #[test]
    fn sign_is_deterministic_and_binds_name_and_nonce() {
        let a = sign("k", "sar_snt3", "n1");
        assert_eq!(a, sign("k", "sar_snt3", "n1"));
        assert_ne!(a, sign("k", "sar_snt3", "n2"));
        assert_ne!(a, sign("k", "sar_snt4", "n1"));
        assert_ne!(a, sign("other", "sar_snt3", "n1"));
    }

    #[test]
    fn verify_accepts_valid_rejects_tamper() {
        assert!(verify("k", "st", "n", &sign("k", "st", "n")));
        assert!(!verify("k", "st", "n", &sign("k", "st", "x")));
        assert!(!verify("wrong", "st", "n", &sign("k", "st", "n")));
        // 长度不同也必须安全返回 false（恒定时间比较不能 panic）
        assert!(!verify("k", "st", "n", "deadbeef"));
    }

    #[test]
    fn nonce_is_64_hex_chars_and_unique() {
        let n1 = new_nonce();
        let n2 = new_nonce();
        assert_eq!(n1.len(), 64);
        assert!(n1.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(n1, n2);
    }

    #[test]
    fn connect_url_appends_query_with_encoding() {
        let url = connect_url("ws://127.0.0.1:9100/ws", "st", "ab", "ff");
        assert_eq!(
            url,
            "ws://127.0.0.1:9100/ws?strategy_name=st&nonce=ab&signature=ff"
        );
        let url2 = connect_url("ws://h/ws?x=1", "a b", "n", "s");
        assert!(url2.starts_with("ws://h/ws?x=1&strategy_name=a%20b&nonce=n&signature=s"));
    }
}
