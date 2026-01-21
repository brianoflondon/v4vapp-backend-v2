from v4vapp_backend_v2.accounting.ledger_type_class import (
    LedgerType,
    LedgerTypeDetails,
    LedgerTypeIcon,
    LedgerTypeStr,
    ledger_type_details_for_value,
    list_all_ledger_type_details,
)


def test_labels_match_configured_mapping():
    """For each configured human label, the details lookup should return the same label."""
    for lt, expected_label in LedgerTypeStr.items():
        d = ledger_type_details_for_value(lt.value)
        assert d is not None
        assert isinstance(d, LedgerTypeDetails)
        assert d.label == expected_label


def test_icons_match_configured_mapping():
    """For each configured icon mapping, the details lookup should return the same icon."""
    for lt, expected_icon in LedgerTypeIcon.items():
        d = ledger_type_details_for_value(lt.value)
        assert d is not None
        assert isinstance(d, LedgerTypeDetails)
        assert d.icon == expected_icon


def test_missing_label_falls_back_to_value_and_missing_icon_is_blank():
    """Choose ledger types that have no configured label/icon and assert fallback behaviour.

    The test does not rely on exact enum values; it finds members dynamically.
    """
    # Pick a ledger type with no configured label (if any)
    no_label_candidates = [lt for lt in LedgerType if lt not in LedgerTypeStr]
    assert no_label_candidates, (
        "No ledger types are missing a configured label; adjust test if mappings cover all enums"
    )
    lt_no_label = no_label_candidates[0]

    d = ledger_type_details_for_value(lt_no_label.value)
    assert d is not None
    # label should be the exact enum value (unchanged)
    assert d.label == lt_no_label.value

    # Pick a ledger type with no configured icon (if any)
    no_icon_candidates = [lt for lt in LedgerType if lt not in LedgerTypeIcon]
    assert no_icon_candidates, (
        "No ledger types are missing a configured icon; adjust test if mappings cover all enums"
    )
    lt_no_icon = no_icon_candidates[0]

    d2 = ledger_type_details_for_value(lt_no_icon.value)
    assert d2 is not None
    assert d2.icon == ""


def test_lookup_by_unknown_value_returns_none():
    assert ledger_type_details_for_value("this_value_does_not_exist") is None


def test_list_all_returns_all_enum_members():
    all_details = list_all_ledger_type_details()
    # ensure we return one detail per LedgerType member
    assert len(all_details) == len(list(LedgerType))
    assert all(isinstance(d, LedgerTypeDetails) for d in all_details)
    # ensure all ledger_type values are unique
    values = {d.ledger_type for d in all_details}
    assert values == set(LedgerType)


def test_capitalized_property_is_available_and_matches_enum():
    details = list_all_ledger_type_details()
    for d in details:
        assert hasattr(d, "capitalized")
        assert isinstance(d.capitalized, str)
        assert d.capitalized == d.ledger_type.capitalized
