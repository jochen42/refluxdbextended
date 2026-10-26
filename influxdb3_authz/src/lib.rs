use std::sync::Arc;

use async_trait::async_trait;
use authz::{
    Action as IoxAction, Authorization, Authorizer as IoxAuthorizer, Error as IoxError,
    Permission as IoxPermission, Resource as IoxResource, Target as IoxTarget,
};
use influxdb3_id::{DbId, TokenId};
use iox_time::{Time, TimeProvider};
use observability_deps::tracing::{debug, trace};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::fmt::Debug;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatabaseActions(pub u16);

impl DatabaseActions {
    pub const READ: Self = Self(1);
    pub const WRITE: Self = Self(1 << 1);
    pub const CREATE: Self = Self(1 << 2);

    /// True when every action bit in `other` is also set in `self`.
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for DatabaseActions {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl From<u16> for DatabaseActions {
    fn from(value: u16) -> Self {
        DatabaseActions(value)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CrudActions(pub u16);

impl CrudActions {
    pub const READ: Self = Self(1);
    pub const WRITE: Self = Self(1 << 1);
    pub const CREATE: Self = Self(1 << 2);
    pub const DELETE: Self = Self(1 << 3);

    /// True when every action bit in `other` is also set in `self`.
    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl std::ops::BitOr for CrudActions {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl From<u16> for CrudActions {
    fn from(value: u16) -> Self {
        CrudActions(value)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AccessRequest {
    /// Access to an existing database.
    Database(DbId, DatabaseActions),
    /// Access to a database that is not (yet) in the catalog, e.g. a write that
    /// auto-creates the database. Only a wildcard database grant can match it,
    /// because a grant for specific databases is stored by id.
    UnknownDatabase(DatabaseActions),
    Token(TokenId, CrudActions),
    /// Operator-level access: configure endpoints, token management, plugins.
    Admin,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ResourceAuthorizationError {
    #[error("unauthorized to perform requested action with the token")]
    Unauthorized,

    #[error("resource type not supported, {0}")]
    ResourceNotSupported(String),
}

#[derive(Debug, thiserror::Error)]
pub enum AuthenticatorError {
    /// Error for token that is present in the request but missing in the catalog
    #[error("token provided is not present in catalog")]
    InvalidToken,
    /// Error for token that has expired
    #[error("token has expired {0}")]
    ExpiredToken(String),
    /// Error for missing token (this should really be handled at the HTTP/Grpc API layer itself)
    #[error("missing token to authenticate")]
    MissingToken,
}

impl From<AuthenticatorError> for IoxError {
    fn from(err: AuthenticatorError) -> Self {
        match err {
            AuthenticatorError::InvalidToken => IoxError::NoToken,
            AuthenticatorError::ExpiredToken(token_expiry_time) => {
                // there is no mapping to let the caller know about expired token in iox so
                // we just log it for now (only useful in debugging)
                debug!(?token_expiry_time, "supplied token has expired");
                IoxError::InvalidToken
            }
            AuthenticatorError::MissingToken => IoxError::NoToken,
        }
    }
}

#[async_trait]
pub trait AuthProvider: Debug + Send + Sync + 'static {
    /// Authenticate request, at this point token maybe missing
    async fn authenticate(
        &self,
        unhashed_token: Option<Vec<u8>>,
    ) -> Result<TokenId, AuthenticatorError>;

    /// Authorize an action for an already authenticated request
    async fn authorize_action(
        &self,
        // you must have a token if you're trying to authorize request
        token_id: &TokenId,
        access_request: AccessRequest,
    ) -> Result<(), ResourceAuthorizationError>;

    // When authenticating we fetch the token_id and add it to http req extensions and
    // later lookup extension. This extra indicator allows to check if the missing token
    // in extension is an authentication issue or not, otherwise need to pass a flag to indicate
    // whether server has been started with auth flag or not
    fn should_check_token(&self) -> bool;

    /// this is only needed so that grpc service can get hold of a `Arc<dyn IoxAuthorizer>`
    fn upcast(&self) -> Arc<dyn IoxAuthorizer>;
}

pub trait TokenProvider: Send + Debug + Sync + 'static {
    fn get_token(&self, token_hash: Vec<u8>) -> Option<Arc<TokenInfo>>;

    fn get_token_by_id(&self, token_id: &TokenId) -> Option<Arc<TokenInfo>>;

    /// Resolve a database name to its id. Token grants for specific databases
    /// are stored by id, so name-based requests are resolved through this.
    fn db_name_to_id(&self, db_name: &str) -> Option<DbId>;
}

#[derive(Clone, Debug)]
pub struct TokenAuthenticator {
    token_provider: Arc<dyn TokenProvider>,
    time_provider: Arc<dyn TimeProvider>,
}

impl TokenAuthenticator {
    pub fn new(
        token_provider: Arc<dyn TokenProvider>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            token_provider,
            time_provider,
        }
    }
}

impl TokenAuthenticator {
    /// Look up and validate (existence, expiry) the token for a raw secret.
    fn authenticate_token(
        &self,
        unhashed_token: Option<&[u8]>,
    ) -> Result<Arc<TokenInfo>, AuthenticatorError> {
        let provided = unhashed_token.ok_or(AuthenticatorError::MissingToken)?;
        let hashed_token = Sha512::digest(provided);
        if let Some(token) = self.token_provider.get_token(hashed_token.to_vec()) {
            let expiry_ms = token.expiry_millis();
            let current_timestamp_ms = self.time_provider.now().timestamp_millis();
            debug!(?expiry_ms, ?current_timestamp_ms, "time comparison");
            let is_active = expiry_ms > current_timestamp_ms;
            if is_active {
                return Ok(token);
            } else {
                trace!(token_expiry = ?expiry_ms, "token has expired");
                let formatted = Time::from_timestamp_millis(expiry_ms)
                    .map(|time| time.to_rfc3339())
                    // this should be an error but we're within an error already,
                    // also it's only useful for debugging this error so a string
                    // default is ok here.
                    .unwrap_or("CannotBuildTime".to_owned());
                return Err(AuthenticatorError::ExpiredToken(formatted));
            }
        };
        Err(AuthenticatorError::InvalidToken)
    }

    /// Map an IOx permission (used by the v1 query API and Flight) onto a
    /// token grant check.
    fn grants_iox_permission(&self, token: &TokenInfo, permission: &IoxPermission) -> bool {
        let IoxPermission::ResourceAction(IoxResource::Database(target), action) = permission;
        let wanted = match action {
            IoxAction::Read | IoxAction::ReadSchema | IoxAction::Describe => DatabaseActions::READ,
            IoxAction::Write => DatabaseActions::WRITE,
            IoxAction::Create => DatabaseActions::CREATE,
            // Deleting a database is operator-level.
            IoxAction::Delete => return token.permits(&AccessRequest::Admin),
        };
        let request = match target {
            IoxTarget::ResourceName(name) => match self.token_provider.db_name_to_id(name) {
                Some(db_id) => AccessRequest::Database(db_id, wanted),
                None => AccessRequest::UnknownDatabase(wanted),
            },
            IoxTarget::ResourceId(_) => AccessRequest::UnknownDatabase(wanted),
        };
        token.permits(&request)
    }
}

#[async_trait]
impl AuthProvider for TokenAuthenticator {
    async fn authenticate(
        &self,
        unhashed_token: Option<Vec<u8>>,
    ) -> Result<TokenId, AuthenticatorError> {
        self.authenticate_token(unhashed_token.as_deref())
            .map(|token| token.id)
    }

    async fn authorize_action(
        &self,
        token_id: &TokenId,
        access_request: AccessRequest,
    ) -> Result<(), ResourceAuthorizationError> {
        // The token may have been deleted between authentication and here.
        let token = self
            .token_provider
            .get_token_by_id(token_id)
            .ok_or(ResourceAuthorizationError::Unauthorized)?;
        if token.permits(&access_request) {
            Ok(())
        } else {
            debug!(
                ?token_id,
                ?access_request,
                "token lacks permission for request"
            );
            Err(ResourceAuthorizationError::Unauthorized)
        }
    }

    fn should_check_token(&self) -> bool {
        true
    }

    fn upcast(&self) -> Arc<dyn IoxAuthorizer> {
        let cloned_self = (*self).clone();
        Arc::new(cloned_self) as _
    }
}

#[async_trait]
impl IoxAuthorizer for TokenAuthenticator {
    async fn authorize(
        &self,
        token: Option<Vec<u8>>,
        perms: &[IoxPermission],
    ) -> Result<Authorization, IoxError> {
        let token_info = self.authenticate_token(token.as_deref())?;
        // Per the `Authorizer` contract, return the subset of requested permissions the token
        // grants, and `Forbidden` when it grants none of them. Callers such as SHOW DATABASES
        // rely on the subset to filter their output.
        //
        // (trevor) The "subject" may just use the `token_id` as a string, but we withheld doing so
        // in order to decided and ensure that we make use of the subject consistently throughout
        // the system. See: https://github.com/influxdata/influxdb/issues/26440
        if perms.is_empty() {
            return Ok(Authorization::new(None, vec![]));
        }
        let granted: Vec<IoxPermission> = perms
            .iter()
            .filter(|perm| self.grants_iox_permission(&token_info, perm))
            .cloned()
            .collect();
        if granted.is_empty() {
            debug!(token_id = ?token_info.id, ?perms, "token lacks all requested permissions");
            return Err(IoxError::Forbidden {
                authorization: Authorization::new(None, vec![]),
            });
        }
        Ok(Authorization::new(None, granted))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NoAuthAuthenticator;

#[async_trait]
impl IoxAuthorizer for NoAuthAuthenticator {
    async fn authorize(
        &self,
        _token: Option<Vec<u8>>,
        perms: &[IoxPermission],
    ) -> Result<Authorization, IoxError> {
        Ok(Authorization::new(None, perms.to_vec()))
    }

    async fn probe(&self) -> Result<(), IoxError> {
        Ok(())
    }
}

#[async_trait]
impl AuthProvider for NoAuthAuthenticator {
    async fn authenticate(
        &self,
        _unhashed_token: Option<Vec<u8>>,
    ) -> Result<TokenId, AuthenticatorError> {
        Ok(TokenId::from(0))
    }

    async fn authorize_action(
        &self,
        _token_id: &TokenId,
        _access_request: AccessRequest,
    ) -> Result<(), ResourceAuthorizationError> {
        Ok(())
    }

    fn should_check_token(&self) -> bool {
        false
    }

    fn upcast(&self) -> Arc<dyn IoxAuthorizer> {
        Arc::new(*self) as _
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TokenInfo {
    pub id: TokenId,
    pub name: Arc<str>,
    pub hash: Vec<u8>,
    pub description: Option<String>,
    pub created_by: Option<TokenId>,
    pub created_at: i64,
    pub updated_at: Option<i64>,
    pub updated_by: Option<TokenId>,
    pub expiry_millis: i64,
    // should be used only in enterprise
    pub permissions: Vec<Permission>,
}

impl TokenInfo {
    pub fn new(
        id: TokenId,
        name: Arc<str>,
        hash: Vec<u8>,
        created_at: i64,
        expiry_millis: Option<i64>,
    ) -> Self {
        Self {
            id,
            name,
            hash,
            created_at,
            expiry_millis: expiry_millis.unwrap_or(i64::MAX),
            description: None,
            created_by: None,
            updated_at: None,
            updated_by: None,
            permissions: Default::default(),
        }
    }

    pub fn expiry_millis(&self) -> i64 {
        self.expiry_millis
    }

    pub fn maybe_expiry_millis(&self) -> Option<i64> {
        if self.expiry_millis == i64::MAX {
            return None;
        }
        Some(self.expiry_millis)
    }

    pub fn set_permissions(&mut self, all_permissions: Vec<Permission>) {
        self.permissions = all_permissions;
    }

    /// True when at least one of the token's permissions grants `request`.
    pub fn permits(&self, request: &AccessRequest) -> bool {
        self.permissions.iter().any(|p| p.grants(request))
    }

    pub fn is_admin(&self) -> bool {
        self.permissions.iter().any(Permission::is_admin)
    }
}

// common types

/// This permission should map exactly to any of these variations
///   --permission "db:db1,db2:read"
///   --permission "db:*:read"
///   --permission "db:db1:read,write"
///   --permission "db:*:read,write"
///   --permission "*:*:read,write" (not sure if this is useful?)
///   --permission "*:*:*" (this will be admin)
///
/// These permissions are loosely translated to ResourceType, ResourceIdentifier and Actions. There
/// are more restrictive ways to handle it to disallow certain combinations using type system
/// itself. The wildcards at each level makes the setup harder to model everything within a single
/// enum for example. So, once we add few more resources this should be fairly straightforward to
/// refine to make certain combinations impossible to set
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Permission {
    pub resource_type: ResourceType,
    pub resource_identifier: ResourceIdentifier,
    pub actions: Actions,
}

impl Permission {
    /// The `*:*:*` grant that admin tokens carry.
    pub fn is_admin(&self) -> bool {
        matches!(
            (self.resource_type, &self.resource_identifier, self.actions),
            (
                ResourceType::Wildcard,
                ResourceIdentifier::Wildcard,
                Actions::Wildcard
            )
        )
    }

    /// Whether this single permission covers `request`.
    pub fn grants(&self, request: &AccessRequest) -> bool {
        if self.is_admin() {
            return true;
        }
        match *request {
            AccessRequest::Admin => false,
            AccessRequest::Database(db_id, wanted) => {
                self.resource_type == ResourceType::Database
                    && match &self.resource_identifier {
                        ResourceIdentifier::Wildcard => true,
                        ResourceIdentifier::Database(ids) => ids.contains(&db_id),
                        ResourceIdentifier::Token(_) => false,
                    }
                    && self.grants_database_actions(wanted)
            }
            AccessRequest::UnknownDatabase(wanted) => {
                self.resource_type == ResourceType::Database
                    && self.resource_identifier == ResourceIdentifier::Wildcard
                    && self.grants_database_actions(wanted)
            }
            AccessRequest::Token(token_id, wanted) => {
                self.resource_type == ResourceType::Token
                    && match &self.resource_identifier {
                        ResourceIdentifier::Wildcard => true,
                        ResourceIdentifier::Token(ids) => ids.contains(&token_id),
                        ResourceIdentifier::Database(_) => false,
                    }
                    && match self.actions {
                        Actions::Wildcard => true,
                        Actions::Token(have) => have.contains(wanted),
                        Actions::Database(_) => false,
                    }
            }
        }
    }

    fn grants_database_actions(&self, wanted: DatabaseActions) -> bool {
        match self.actions {
            Actions::Wildcard => true,
            Actions::Database(have) => have.contains(wanted),
            Actions::Token(_) => false,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ResourceType {
    Database,
    Token,
    Wildcard,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum ResourceIdentifier {
    Database(Vec<DbId>),
    Token(Vec<TokenId>),
    Wildcard,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Actions {
    Database(DatabaseActions),
    Token(CrudActions),
    Wildcard,
}

#[cfg(test)]
mod tests;
