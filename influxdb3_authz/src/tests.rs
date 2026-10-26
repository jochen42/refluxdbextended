use std::sync::Arc;

use influxdb3_id::{DbId, TokenId};
use iox_time::{MockProvider, Time};
use sha2::Digest;

use crate::{
    AccessRequest, Actions, AuthProvider, AuthenticatorError, CrudActions, DatabaseActions,
    Permission, ResourceIdentifier, ResourceType, TokenAuthenticator, TokenInfo, TokenProvider,
};

#[derive(Debug)]
struct MockTokenProvider {
    hashed_token: Vec<u8>,
    expired: bool,
}

impl MockTokenProvider {
    fn new(token: &str, expired: bool) -> Self {
        let hash = sha2::Sha512::digest(token);
        Self {
            hashed_token: hash.to_vec(),
            expired,
        }
    }
}

impl TokenProvider for MockTokenProvider {
    fn get_token(&self, token_hash: Vec<u8>) -> Option<std::sync::Arc<crate::TokenInfo>> {
        if token_hash == self.hashed_token {
            if self.expired {
                Some(Arc::new(TokenInfo::new(
                    TokenId::from(0),
                    "admin-token".into(),
                    self.hashed_token.clone(),
                    1000,
                    Some(1743320379000),
                )))
            } else {
                Some(Arc::new(TokenInfo::new(
                    TokenId::from(0),
                    "admin-token".into(),
                    self.hashed_token.clone(),
                    1000,
                    Some(i64::MAX),
                )))
            }
        } else {
            None
        }
    }

    fn get_token_by_id(&self, _token_id: &TokenId) -> Option<Arc<TokenInfo>> {
        None
    }

    fn db_name_to_id(&self, _db_name: &str) -> Option<DbId> {
        None
    }
}

#[test_log::test(tokio::test)]
async fn test_authenticator_success() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));
    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let token_id = authenticator
        .authenticate(Some(token.as_bytes().to_vec()))
        .await
        .expect("to get token id after successful auth");
    assert_eq!(TokenId::from(0), token_id);
}

#[test_log::test(tokio::test)]
async fn test_authenticator_missing_token() {
    let time_provider = MockProvider::new(Time::from_timestamp_nanos(0));
    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, false);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let result = authenticator
        .authenticate(Some("not-matching-token".as_bytes().to_vec()))
        .await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        AuthenticatorError::InvalidToken
    ));
}

#[test_log::test(tokio::test)]
async fn test_authenticator_expired_token() {
    let time_provider = MockProvider::new(Time::from_timestamp_millis(1743420379000).unwrap());
    let token = "sample-token";
    let token_provider = MockTokenProvider::new(token, true);
    let authenticator = TokenAuthenticator::new(Arc::new(token_provider), Arc::new(time_provider));
    let result = authenticator
        .authenticate(Some("sample-token".as_bytes().to_vec()))
        .await;
    assert!(result.is_err());
    if let AuthenticatorError::ExpiredToken(expiry_time_str) = result.unwrap_err() {
        assert_eq!("2025-03-30T07:39:39+00:00", expiry_time_str);
    } else {
        panic!("not the right type of authentication error");
    }
}

// ---------------------------------------------------------------------------
// Permission enforcement
// ---------------------------------------------------------------------------

const DB_A: DbId = DbId::new(1);
const DB_B: DbId = DbId::new(2);

fn db_perm(ids: Option<Vec<DbId>>, actions: DatabaseActions) -> Permission {
    Permission {
        resource_type: ResourceType::Database,
        resource_identifier: ids
            .map(ResourceIdentifier::Database)
            .unwrap_or(ResourceIdentifier::Wildcard),
        actions: Actions::Database(actions),
    }
}

fn admin_perm() -> Permission {
    Permission {
        resource_type: ResourceType::Wildcard,
        resource_identifier: ResourceIdentifier::Wildcard,
        actions: Actions::Wildcard,
    }
}

fn token_with(id: u64, secret: &str, permissions: Vec<Permission>) -> TokenInfo {
    let mut info = TokenInfo::new(
        TokenId::from(id),
        format!("token-{id}").into(),
        sha2::Sha512::digest(secret).to_vec(),
        0,
        None,
    );
    info.set_permissions(permissions);
    info
}

/// Token provider holding several tokens and a name -> id database map.
#[derive(Debug, Default)]
struct MultiTokenProvider {
    tokens: Vec<Arc<TokenInfo>>,
    dbs: Vec<(&'static str, DbId)>,
}

impl TokenProvider for MultiTokenProvider {
    fn get_token(&self, token_hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        self.tokens.iter().find(|t| t.hash == token_hash).cloned()
    }

    fn get_token_by_id(&self, token_id: &TokenId) -> Option<Arc<TokenInfo>> {
        self.tokens.iter().find(|t| t.id == *token_id).cloned()
    }

    fn db_name_to_id(&self, db_name: &str) -> Option<DbId> {
        self.dbs
            .iter()
            .find(|(name, _)| *name == db_name)
            .map(|(_, id)| *id)
    }
}

fn authenticator(tokens: Vec<TokenInfo>) -> TokenAuthenticator {
    let provider = MultiTokenProvider {
        tokens: tokens.into_iter().map(Arc::new).collect(),
        dbs: vec![("a", DB_A), ("b", DB_B)],
    };
    TokenAuthenticator::new(
        Arc::new(provider),
        Arc::new(MockProvider::new(Time::from_timestamp_nanos(0))),
    )
}

#[test]
fn read_only_token_permits_only_reads_on_its_database() {
    let token = token_with(
        1,
        "ro",
        vec![db_perm(Some(vec![DB_A]), DatabaseActions::READ)],
    );

    assert!(token.permits(&AccessRequest::Database(DB_A, DatabaseActions::READ)));
    assert!(!token.permits(&AccessRequest::Database(DB_A, DatabaseActions::WRITE)));
    assert!(!token.permits(&AccessRequest::Database(DB_B, DatabaseActions::READ)));
    assert!(!token.permits(&AccessRequest::UnknownDatabase(DatabaseActions::READ)));
    assert!(!token.permits(&AccessRequest::Admin));
    assert!(!token.is_admin());
}

#[test]
fn wildcard_read_token_reads_everything_writes_nothing() {
    let token = token_with(1, "ro", vec![db_perm(None, DatabaseActions::READ)]);

    assert!(token.permits(&AccessRequest::Database(DB_A, DatabaseActions::READ)));
    assert!(token.permits(&AccessRequest::Database(DB_B, DatabaseActions::READ)));
    assert!(token.permits(&AccessRequest::UnknownDatabase(DatabaseActions::READ)));
    assert!(!token.permits(&AccessRequest::Database(DB_A, DatabaseActions::WRITE)));
    assert!(!token.permits(&AccessRequest::UnknownDatabase(DatabaseActions::WRITE)));
    assert!(!token.permits(&AccessRequest::Admin));
}

#[test]
fn read_write_token_needs_all_requested_bits() {
    let rw = DatabaseActions::READ | DatabaseActions::WRITE;
    let token = token_with(1, "rw", vec![db_perm(Some(vec![DB_A]), rw)]);

    assert!(token.permits(&AccessRequest::Database(DB_A, DatabaseActions::WRITE)));
    assert!(token.permits(&AccessRequest::Database(DB_A, rw)));
    assert!(!token.permits(&AccessRequest::Database(DB_A, rw | DatabaseActions::CREATE)));
}

#[test]
fn admin_token_permits_everything() {
    let token = token_with(1, "admin", vec![admin_perm()]);

    assert!(token.is_admin());
    assert!(token.permits(&AccessRequest::Admin));
    assert!(token.permits(&AccessRequest::Database(DB_A, DatabaseActions::WRITE)));
    assert!(token.permits(&AccessRequest::UnknownDatabase(DatabaseActions::CREATE)));
    assert!(token.permits(&AccessRequest::Token(TokenId::from(7), CrudActions::DELETE)));
}

#[test]
fn token_without_permissions_permits_nothing() {
    let token = token_with(1, "none", vec![]);
    assert!(!token.permits(&AccessRequest::Database(DB_A, DatabaseActions::READ)));
    assert!(!token.permits(&AccessRequest::Admin));
}

#[test]
fn token_grants_do_not_leak_into_databases() {
    let token = token_with(
        1,
        "tok",
        vec![Permission {
            resource_type: ResourceType::Token,
            resource_identifier: ResourceIdentifier::Wildcard,
            actions: Actions::Wildcard,
        }],
    );
    assert!(token.permits(&AccessRequest::Token(TokenId::from(3), CrudActions::READ)));
    assert!(!token.permits(&AccessRequest::Database(DB_A, DatabaseActions::READ)));
    assert!(!token.permits(&AccessRequest::Admin));
}

#[test_log::test(tokio::test)]
async fn authorize_action_enforces_token_permissions() {
    let auth = authenticator(vec![
        token_with(
            1,
            "ro",
            vec![db_perm(Some(vec![DB_A]), DatabaseActions::READ)],
        ),
        token_with(2, "admin", vec![admin_perm()]),
    ]);
    let ro = TokenId::from(1);
    let admin = TokenId::from(2);

    auth.authorize_action(&ro, AccessRequest::Database(DB_A, DatabaseActions::READ))
        .await
        .expect("read allowed");
    auth.authorize_action(&ro, AccessRequest::Database(DB_A, DatabaseActions::WRITE))
        .await
        .expect_err("write denied");
    auth.authorize_action(&ro, AccessRequest::Admin)
        .await
        .expect_err("admin denied");
    auth.authorize_action(&admin, AccessRequest::Admin)
        .await
        .expect("admin allowed");
    // A token id that no longer exists (deleted after authentication).
    auth.authorize_action(&TokenId::from(99), AccessRequest::Admin)
        .await
        .expect_err("unknown token denied");
}

#[test_log::test(tokio::test)]
async fn iox_authorize_returns_granted_subset_or_forbidden() {
    use authz::{
        Action, Authorizer, Error as IoxError, Permission as IoxPermission, Resource, Target,
    };

    let auth = authenticator(vec![token_with(
        1,
        "ro",
        vec![db_perm(Some(vec![DB_A]), DatabaseActions::READ)],
    )]);
    let perm = |db: &str, action| {
        IoxPermission::ResourceAction(Resource::Database(Target::ResourceName(db.into())), action)
    };
    let secret = Some(b"ro".to_vec());

    // SHOW DATABASES style: only the readable database survives.
    let granted = auth
        .authorize(
            secret.clone(),
            &[perm("a", Action::Describe), perm("b", Action::Describe)],
        )
        .await
        .expect("partial grant");
    assert_eq!(granted.permissions(), &vec![perm("a", Action::Describe)]);

    // Read query on db a is allowed.
    auth.authorize(secret.clone(), &[perm("a", Action::Read)])
        .await
        .expect("read allowed");

    // Writes, deletes, and other databases are forbidden.
    for denied in [
        perm("a", Action::Write),
        perm("a", Action::Delete),
        perm("b", Action::Read),
        perm("missing", Action::Read),
    ] {
        let err = auth
            .authorize(secret.clone(), std::slice::from_ref(&denied))
            .await
            .expect_err("denied");
        assert!(
            matches!(err, IoxError::Forbidden { .. }),
            "{denied:?}: {err:?}"
        );
    }

    // Unknown secret is still an authentication failure, not Forbidden.
    let err = auth
        .authorize(Some(b"nope".to_vec()), &[perm("a", Action::Read)])
        .await
        .expect_err("invalid token");
    assert!(matches!(err, IoxError::NoToken | IoxError::InvalidToken));
}
