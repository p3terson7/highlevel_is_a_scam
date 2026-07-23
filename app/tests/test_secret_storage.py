from cryptography.fernet import Fernet
import pytest

from app.core.config import Settings
from app.services.secret_storage import (
    SecretStorageError,
    is_protected_secret,
    protect_secret,
    reveal_secret,
    rotate_protected_secret,
    validate_secret_storage_settings,
)


def test_secret_storage_encrypts_and_decrypts_with_dedicated_key():
    key = Fernet.generate_key().decode("ascii")
    settings = Settings(
        admin_token="a-secure-random-admin-token-over-32-chars",
        settings_encryption_keys=key,
    )

    stored = protect_secret("provider-secret", settings=settings)

    assert is_protected_secret(stored)
    assert "provider-secret" not in stored
    assert reveal_secret(stored, settings=settings) == "provider-secret"


def test_secret_storage_supports_rotation_key_order():
    old_key = Fernet.generate_key().decode("ascii")
    new_key = Fernet.generate_key().decode("ascii")
    old_settings = Settings(
        admin_token="a-secure-random-admin-token-over-32-chars",
        settings_encryption_keys=old_key,
    )
    rotated_settings = Settings(
        admin_token="a-secure-random-admin-token-over-32-chars",
        settings_encryption_keys=f"{new_key},{old_key}",
    )
    stored = protect_secret("rotatable-secret", settings=old_settings)

    assert reveal_secret(stored, settings=rotated_settings) == "rotatable-secret"
    rewritten = protect_secret(stored, settings=rotated_settings)
    assert reveal_secret(rewritten, settings=rotated_settings) == "rotatable-secret"
    with pytest.raises(SecretStorageError):
        reveal_secret(
            rewritten,
            settings=Settings(
                admin_token="a-secure-random-admin-token-over-32-chars",
                settings_encryption_keys=old_key,
            ),
        )


def test_plaintext_legacy_secret_remains_readable_during_migration():
    assert reveal_secret("legacy-plaintext") == "legacy-plaintext"


def test_admin_derived_ciphertext_rotates_to_dedicated_key_before_admin_rotation():
    dedicated_key = Fernet.generate_key().decode("ascii")
    legacy_settings = Settings(
        env="dev",
        admin_token="original-admin-token-over-thirty-two-characters",
        settings_encryption_keys="",
    )
    migration_settings = Settings(
        env="production",
        admin_token="original-admin-token-over-thirty-two-characters",
        settings_encryption_keys=dedicated_key,
    )
    rotated_admin_settings = Settings(
        env="production",
        admin_token="rotated-admin-token-over-thirty-two-characters!",
        settings_encryption_keys=dedicated_key,
    )

    legacy_ciphertext = protect_secret("legacy-admin-derived-secret", settings=legacy_settings)
    migrated_ciphertext = rotate_protected_secret(
        legacy_ciphertext,
        settings=migration_settings,
    )

    assert migrated_ciphertext != legacy_ciphertext
    assert (
        reveal_secret(migrated_ciphertext, settings=rotated_admin_settings)
        == "legacy-admin-derived-secret"
    )


def test_production_requires_dedicated_settings_encryption_key():
    with pytest.raises(SecretStorageError):
        validate_secret_storage_settings(
            Settings(
                env="production",
                admin_token="a-secure-random-admin-token-over-32-chars",
                settings_encryption_keys="",
            )
        )
