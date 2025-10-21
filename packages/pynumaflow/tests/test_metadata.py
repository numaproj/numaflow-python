import pytest
from pynumaflow._metadata import UserMetadata, SystemMetadata


class TestSystemMetadata:
    """Tests for SystemMetadata (read-only)"""

    def test_empty_system_metadata(self):
        """Test empty SystemMetadata"""
        metadata = SystemMetadata()
        assert metadata.groups() == []
        assert metadata.keys("any_group") == []
        assert metadata.value("any_group", "any_key") is None

    def test_system_metadata_groups(self):
        """Test groups() method"""
        metadata = SystemMetadata(
            _data={"group1": {"key1": b"value1"}, "group2": {"key2": b"value2"}}
        )
        groups = metadata.groups()
        assert len(groups) == 2
        assert "group1" in groups
        assert "group2" in groups

    def test_system_metadata_keys_existing_group(self):
        """Test keys() method with existing group"""
        metadata = SystemMetadata(_data={"group1": {"key1": b"value1", "key2": b"value2"}})
        keys = metadata.keys("group1")
        assert len(keys) == 2
        assert "key1" in keys
        assert "key2" in keys

    def test_system_metadata_keys_nonexistent_group(self):
        """Test keys() method with non-existent group"""
        metadata = SystemMetadata(_data={"group1": {"key1": b"value1"}})
        keys = metadata.keys("nonexistent")
        assert keys == []

    def test_system_metadata_value_existing(self):
        """Test value() method with existing group and key"""
        metadata = SystemMetadata(_data={"group1": {"key1": b"value1"}})
        assert metadata.value("group1", "key1") == b"value1"

    def test_system_metadata_value_nonexistent_group(self):
        """Test value() method with non-existent group"""
        metadata = SystemMetadata(_data={"group1": {"key1": b"value1"}})
        assert metadata.value("nonexistent", "key1") is None

    def test_system_metadata_value_nonexistent_key(self):
        """Test value() method with non-existent key"""
        metadata = SystemMetadata(_data={"group1": {"key1": b"value1"}})
        assert metadata.value("group1", "nonexistent") is None

    def test_system_metadata_value_nonexistent_both(self):
        """Test value() method with non-existent group and key"""
        metadata = SystemMetadata()
        assert metadata.value("nonexistent", "nonexistent") is None


class TestUserMetadata:
    """Tests for UserMetadata (read-write)"""

    def test_empty_user_metadata(self):
        """Test empty UserMetadata"""
        metadata = UserMetadata()
        assert metadata.groups() == []
        assert len(metadata) == 0
        assert metadata.keys("any_group") is None
        assert metadata.value("any_group", "any_key") is None

    def test_user_metadata_groups(self):
        """Test groups() method"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group2", "key2", b"value2")
        groups = metadata.groups()
        assert len(groups) == 2
        assert "group1" in groups
        assert "group2" in groups

    def test_user_metadata_keys_existing_group(self):
        """Test keys() method with existing group"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group1", "key2", b"value2")
        keys = metadata.keys("group1")
        assert keys is not None
        assert len(keys) == 2
        assert "key1" in keys
        assert "key2" in keys

    def test_user_metadata_keys_nonexistent_group(self):
        """Test keys() method with non-existent group returns None"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        assert metadata.keys("nonexistent") is None

    def test_user_metadata_contains(self):
        """Test __contains__ method"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        assert "group1" in metadata
        assert "nonexistent" not in metadata

    def test_user_metadata_getitem_existing(self):
        """Test __getitem__ with existing group"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group1", "key2", b"value2")
        group_data = metadata["group1"]
        assert group_data == {"key1": b"value1", "key2": b"value2"}

    def test_user_metadata_getitem_nonexistent_raises_keyerror(self):
        """Test __getitem__ raises KeyError for non-existent group"""
        metadata = UserMetadata()
        with pytest.raises(KeyError):
            _ = metadata["nonexistent"]

    def test_user_metadata_setitem(self):
        """Test __setitem__ method"""
        metadata = UserMetadata()
        metadata["group1"] = {"key1": b"value1", "key2": b"value2"}
        assert metadata["group1"] == {"key1": b"value1", "key2": b"value2"}
        assert len(metadata) == 1

    def test_user_metadata_setitem_overwrite(self):
        """Test __setitem__ overwrites existing group"""
        metadata = UserMetadata()
        metadata["group1"] = {"key1": b"value1"}
        metadata["group1"] = {"key2": b"value2"}
        assert metadata["group1"] == {"key2": b"value2"}
        assert len(metadata) == 1

    def test_user_metadata_delitem_existing(self):
        """Test __delitem__ with existing group"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group2", "key2", b"value2")
        del metadata["group1"]
        assert "group1" not in metadata
        assert "group2" in metadata
        assert len(metadata) == 1

    def test_user_metadata_delitem_nonexistent_raises_keyerror(self):
        """Test __delitem__ raises KeyError for non-existent group"""
        metadata = UserMetadata()
        with pytest.raises(KeyError):
            del metadata["nonexistent"]

    def test_user_metadata_len(self):
        """Test __len__ method"""
        metadata = UserMetadata()
        assert len(metadata) == 0
        metadata.add("group1", "key1", b"value1")
        assert len(metadata) == 1
        metadata.add("group2", "key2", b"value2")
        assert len(metadata) == 2
        metadata.add("group1", "key3", b"value3")
        assert len(metadata) == 2  # Still 2 groups

    def test_user_metadata_value_existing(self):
        """Test value() method with existing group and key"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        assert metadata.value("group1", "key1") == b"value1"

    def test_user_metadata_value_nonexistent_group(self):
        """Test value() method with non-existent group returns None"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        assert metadata.value("nonexistent", "key1") is None

    def test_user_metadata_value_nonexistent_key(self):
        """Test value() method with non-existent key returns None"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        assert metadata.value("group1", "nonexistent") is None

    def test_user_metadata_value_nonexistent_both(self):
        """Test value() method with non-existent group and key returns None"""
        metadata = UserMetadata()
        assert metadata.value("nonexistent", "nonexistent") is None

    def test_user_metadata_add_new_group(self):
        """Test add() method creates new group"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        assert "group1" in metadata
        assert metadata.value("group1", "key1") == b"value1"

    def test_user_metadata_add_to_existing_group(self):
        """Test add() method adds to existing group"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group1", "key2", b"value2")
        assert metadata.value("group1", "key1") == b"value1"
        assert metadata.value("group1", "key2") == b"value2"
        assert len(metadata["group1"]) == 2

    def test_user_metadata_add_overwrites_existing_key(self):
        """Test add() method overwrites existing key"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group1", "key1", b"new_value")
        assert metadata.value("group1", "key1") == b"new_value"
        assert len(metadata["group1"]) == 1

    def test_user_metadata_set_group(self):
        """Test set_group() method"""
        metadata = UserMetadata()
        metadata.set_group("group1", {"key1": b"value1", "key2": b"value2"})
        assert metadata["group1"] == {"key1": b"value1", "key2": b"value2"}

    def test_user_metadata_set_group_overwrites(self):
        """Test set_group() overwrites existing group"""
        metadata = UserMetadata()
        metadata.set_group("group1", {"key1": b"value1"})
        metadata.set_group("group1", {"key2": b"value2"})
        assert metadata["group1"] == {"key2": b"value2"}
        assert "key1" not in metadata["group1"]

    def test_user_metadata_remove_existing_key(self):
        """Test remove() method with existing key"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group1", "key2", b"value2")
        removed_value = metadata.remove("group1", "key1")
        assert removed_value == b"value1"
        assert metadata.value("group1", "key1") is None
        assert metadata.value("group1", "key2") == b"value2"
        assert "group1" in metadata  # Group still exists

    def test_user_metadata_remove_last_key_removes_group(self):
        """Test remove() removes group when last key is removed"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        removed_value = metadata.remove("group1", "key1")
        assert removed_value == b"value1"
        assert "group1" not in metadata

    def test_user_metadata_remove_nonexistent_group(self):
        """Test remove() with non-existent group returns None"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        removed_value = metadata.remove("nonexistent", "key1")
        assert removed_value is None

    def test_user_metadata_remove_nonexistent_key(self):
        """Test remove() with non-existent key returns None and keeps group"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group1", "key2", b"value2")
        removed_value = metadata.remove("group1", "nonexistent")
        assert removed_value is None
        # Group remains because it still has other keys
        assert "group1" in metadata
        assert metadata.value("group1", "key1") == b"value1"
        assert metadata.value("group1", "key2") == b"value2"

    def test_user_metadata_remove_nonexistent_key_single_key_group(self):
        """Test remove() with non-existent key on single-key group keeps the group"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        removed_value = metadata.remove("group1", "nonexistent")
        assert removed_value is None
        # Group remains even though it only has one key and we tried to remove a different one
        assert "group1" in metadata
        assert metadata.value("group1", "key1") == b"value1"

    def test_user_metadata_remove_nonexistent_both(self):
        """Test remove() with non-existent group and key returns None"""
        metadata = UserMetadata()
        removed_value = metadata.remove("nonexistent", "nonexistent")
        assert removed_value is None

    def test_user_metadata_remove_group_existing(self):
        """Test remove_group() with existing group"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group1", "key2", b"value2")
        metadata.add("group2", "key3", b"value3")
        removed_data = metadata.remove_group("group1")
        assert removed_data == {"key1": b"value1", "key2": b"value2"}
        assert "group1" not in metadata
        assert "group2" in metadata

    def test_user_metadata_remove_group_nonexistent(self):
        """Test remove_group() with non-existent group returns None"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        removed_data = metadata.remove_group("nonexistent")
        assert removed_data is None
        assert "group1" in metadata

    def test_user_metadata_clear(self):
        """Test clear() method"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group2", "key2", b"value2")
        metadata.add("group3", "key3", b"value3")
        assert len(metadata) == 3
        metadata.clear()
        assert len(metadata) == 0
        assert metadata.groups() == []

    def test_user_metadata_clear_empty(self):
        """Test clear() on empty metadata"""
        metadata = UserMetadata()
        metadata.clear()
        assert len(metadata) == 0

    def test_user_metadata_to_proto(self):
        """Test _to_proto() method"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"value1")
        metadata.add("group1", "key2", b"value2")
        metadata.add("group2", "key3", b"value3")
        proto = metadata._to_proto()
        assert len(proto.user_metadata) == 2
        assert "group1" in proto.user_metadata
        assert "group2" in proto.user_metadata
        assert proto.user_metadata["group1"].key_value["key1"] == b"value1"
        assert proto.user_metadata["group1"].key_value["key2"] == b"value2"
        assert proto.user_metadata["group2"].key_value["key3"] == b"value3"

    def test_user_metadata_to_proto_empty(self):
        """Test _to_proto() with empty metadata"""
        metadata = UserMetadata()
        proto = metadata._to_proto()
        assert len(proto.user_metadata) == 0

    def test_user_metadata_complex_scenario(self):
        """Test complex scenario with multiple operations"""
        metadata = UserMetadata()

        # Add multiple groups
        metadata.add("headers", "content-type", b"application/json")
        metadata.add("headers", "authorization", b"Bearer token123")
        metadata.add("metrics", "counter", b"42")
        metadata.add("metrics", "timestamp", b"1234567890")

        assert len(metadata) == 2
        assert len(metadata["headers"]) == 2
        assert len(metadata["metrics"]) == 2

        # Remove a key
        metadata.remove("headers", "authorization")
        assert len(metadata["headers"]) == 1
        assert metadata.value("headers", "authorization") is None

        # Remove entire group
        removed = metadata.remove_group("metrics")
        assert removed == {"counter": b"42", "timestamp": b"1234567890"}
        assert "metrics" not in metadata

        # Add new group
        metadata["config"] = {"setting1": b"value1", "setting2": b"value2"}
        assert len(metadata) == 2

        # Clear all
        metadata.clear()
        assert len(metadata) == 0


class TestUserMetadataEdgeCases:
    """Edge cases and special scenarios for UserMetadata"""

    def test_empty_group_name(self):
        """Test with empty string as group name"""
        metadata = UserMetadata()
        metadata.add("", "key1", b"value1")
        assert "" in metadata
        assert metadata.value("", "key1") == b"value1"

    def test_empty_key_name(self):
        """Test with empty string as key name"""
        metadata = UserMetadata()
        metadata.add("group1", "", b"value1")
        assert metadata.value("group1", "") == b"value1"

    def test_empty_value(self):
        """Test with empty bytes as value"""
        metadata = UserMetadata()
        metadata.add("group1", "key1", b"")
        assert metadata.value("group1", "key1") == b""

    def test_multiple_groups_with_same_keys(self):
        """Test different groups can have the same key names"""
        metadata = UserMetadata()
        metadata.add("group1", "key", b"value1")
        metadata.add("group2", "key", b"value2")
        assert metadata.value("group1", "key") == b"value1"
        assert metadata.value("group2", "key") == b"value2"

    def test_special_characters_in_names(self):
        """Test special characters in group and key names"""
        metadata = UserMetadata()
        metadata.add("group-1", "key_1", b"value1")
        metadata.add("group.2", "key:2", b"value2")
        metadata.add("group@3", "key#3", b"value3")
        assert len(metadata) == 3
        assert metadata.value("group-1", "key_1") == b"value1"
        assert metadata.value("group.2", "key:2") == b"value2"
        assert metadata.value("group@3", "key#3") == b"value3"

    def test_large_values(self):
        """Test with large byte values"""
        metadata = UserMetadata()
        large_value = b"x" * 10000
        metadata.add("group1", "large_key", large_value)
        assert metadata.value("group1", "large_key") == large_value

    def test_many_groups(self):
        """Test with many groups"""
        metadata = UserMetadata()
        for i in range(100):
            metadata.add(f"group{i}", f"key{i}", f"value{i}".encode())
        assert len(metadata) == 100
        assert metadata.value("group50", "key50") == b"value50"

    def test_many_keys_in_group(self):
        """Test with many keys in a single group"""
        metadata = UserMetadata()
        for i in range(100):
            metadata.add("group1", f"key{i}", f"value{i}".encode())
        assert len(metadata["group1"]) == 100
        assert metadata.value("group1", "key50") == b"value50"
