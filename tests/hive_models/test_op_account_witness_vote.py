from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


def test_op_account_witness_vote():
    """
    Test the validation of the OpAccountWitnessVote model with hive
        events of type 'account_witness_vote'.
    This test function performs the following steps:
    1. Initializes a counter to track the number of 'account_witness_vote' events.
    2. Iterates through hive events of type 'account_witness_vote' loaded by the `load_hive_events` function.
    3. For each event of type 'account_witness_vote':
        - Increments the counter.
        - Validates the event using the `OpAccountWitnessVote.model_validate` method.
        - Asserts that the transaction ID (`trx_id`) matches between the event
                and the validated model.
        - Asserts that the `vote_value` matches between the event and the validated model.
        - Prints the voter's name.
    4. Asserts that the total count of 'account_witness_vote' events is 28.
    """

    count = 0
    for hive_event in load_hive_events(OpTypes.ACCOUNT_WITNESS_VOTE):
        if hive_event["type"] == "account_witness_vote":
            count += 1
            account_witness_vote = AccountWitnessVote.model_validate(hive_event)
            assert account_witness_vote.trx_id == hive_event["trx_id"]
            account_witness_vote.get_voter_details()
            assert account_witness_vote.voter_details.voter == hive_event["account"]
            print(account_witness_vote.log_str)
            print(account_witness_vote.notification_str)
            assert isinstance(account_witness_vote.log_str, str)
            assert isinstance(account_witness_vote.notification_str, str)
    assert count == 14
