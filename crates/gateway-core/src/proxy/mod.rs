//! Proxy vocabulary shared by the admin plane and the egress engine.
//!
//! The engine (`gateway-proxy`) owns dialing: node selection, leases, cooldowns, and the
//! meow protocol adapters. What lives here is everything the admin plane *names* when it
//! manages `[[proxy_nodes]]` rows:
//!
//! - [`node`] — [`node::ProxyNode`] and its parsed parts, plus the URL parser. Parsing is
//!   pure configuration mapping, so it belongs with the vocabulary rather than with the
//!   dialer;
//! - [`sharelink`] — the share-link dialects (`vmess://BASE64`, `ss://` variants) and the
//!   masking helper used before a link reaches a log or the frontend;
//! - [`pool`] — [`pool::ProxySnapshot`], the installed node set; [`pool::NodeStats`], the
//!   runtime report row; [`pool::ProxyPoolHandle`], the operations the admin plane performs
//!   on the running pool.
//!
//! Why the parser moved down here: the control plane parses stored links during CRUD
//! validation and snapshot loading. Keeping the parser in the engine forced the CRUD crate
//! to depend on the whole egress engine — including its TLS/protocol stack — just to
//! validate a URL string.
//!
//! What deliberately stays in `gateway-proxy`: every operation that needs `reqwest` or
//! `meow` (`ProxyNode::to_reqwest_proxy`, adapter assembly, probing, leases). Those are
//! egress behavior, not vocabulary, and the admin plane never calls them directly — it asks
//! the running pool through [`pool::ProxyPoolHandle`].

pub mod node;
pub mod pool;
pub mod sharelink;

pub use node::{BasicAuth, NodeOpts, ParseError, ProxyNode, ProxyScheme};
pub use pool::{NodeStats, ProxyPoolHandle, ProxySnapshot};
pub use sharelink::{
    ShareLinkBatch, VmessShareLink, mask_link, parse_share_link, parse_share_links,
};
