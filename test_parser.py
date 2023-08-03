from metabase import MetabaseApi

# from json import dumps

TEST_TABLE_NAME = 'DUMMY TABLE'
TEST_TABLE_ID = 111
TEST_FIELD_NAME = 'DUMMY FIELD'
TEST_FIELD_ID = 99999
TEST_CARD_NAME = 'DUMMY CARD'
TEST_CARD_ID = 2222
TEST_DASHBOARD_NAME = 'DUMMY DASHBOARD'
TEST_DASHBOARD_ID = 3333

TEST_DB = {
    "tables": [
        {
            "name": TEST_TABLE_NAME,
            "id": TEST_TABLE_ID,
            "fields": [
                {
                    "id": TEST_FIELD_ID,
                    "name": TEST_FIELD_NAME
                }
            ]
        }
    ]
}

TEST_CARDS = [
    {
        "name": TEST_CARD_NAME,
        "id": TEST_CARD_ID,
    }
]

TEST_DASHBOARDS = [
    {
        "name": TEST_DASHBOARD_NAME,
        "id": TEST_DASHBOARD_ID,
    }
]


def create_test_api():
    api = MetabaseApi('', '', '', debug=True, dry_run=True)
    api.database_export = TEST_DB
    api.cards_export = TEST_CARDS
    api.dashboards_export = TEST_DASHBOARDS

    api.cards_name2id = {}
    for card in TEST_CARDS:
        api.cards_name2id[card["name"]] = card["id"]

    api.dashboards_name2id = {}
    for dashboard in TEST_DASHBOARDS:
        api.dashboards_name2id[dashboard["name"]] = dashboard["id"]

    return api


def test_params_field_field_key():
    obj = {
        "param_fields": {
            f"{TEST_FIELD_ID}": {
                "id": TEST_FIELD_ID,
                "table_id": TEST_TABLE_ID,
            },
        }
    }

    expected = {
        "param_fields": {
            f"%%{TEST_TABLE_NAME}|{TEST_FIELD_NAME}": {
                "field_name": f"%id%{TEST_TABLE_NAME}|{TEST_FIELD_NAME}",
                "table_name": f"%table_id%{TEST_TABLE_NAME}",
            },
        }
    }

    api = create_test_api()
    encoded = api.convert_ids2names('', obj, None)
    # print(dumps(encoded, indent=4))
    assert encoded == expected

    # decoded = api.convert_names2ids('', '', encoded)
    # assert obj == decoded


def test_parameter_mapping_field_array():
    obj = {
        "ordered_cards": [
            {
                "parameter_mappings": [
                    {
                        "parameter_id": "8f8066f5",
                        "card_id": TEST_CARD_ID,
                        "target": [
                            "dimension",
                            [
                                "field",
                                TEST_FIELD_ID,
                                None
                            ]
                        ]
                    }
                ],
            },
        ],
    }

    expected = {
        "ordered_cards": [
            {
                "parameter_mappings": [
                    {
                        "parameter_id": "8f8066f5",
                        "card_name": f"%card_id%{TEST_CARD_NAME}",
                        "target": [
                            "dimension",
                            [
                                "field",
                                f"%%{TEST_TABLE_NAME}|{TEST_FIELD_NAME}",
                                None
                            ]
                        ]
                    }
                ],
            },
        ],
    }

    api = create_test_api()
    encoded = api.convert_ids2names('', obj, None)
    # print(dumps(encoded, indent=4))
    assert encoded == expected

    decoded = api.convert_names2ids('', '', encoded)
    assert obj == decoded


def test_json_keys_and_values():
    obj = {
        "ordered_cards": [
            {
                "visualization_settings": {
                    "click_behavior": {
                        "parameterMapping": {
                            f"[\"dimension\",[\"field\",{TEST_FIELD_ID},null]]": {
                                "target": {
                                    "type": "dimension",
                                    "id": f"[\"dimension\",[\"field\",{TEST_FIELD_ID},null]]",
                                    "dimension": [
                                        "dimension",
                                        [
                                            "field",
                                            TEST_FIELD_ID,
                                            None
                                        ]
                                    ]
                                },
                                "id": f"[\"dimension\",[\"field\",{TEST_FIELD_ID},null]]"
                            }
                        },
                        "targetId": TEST_CARD_ID,
                        "linkType": "question",
                        "type": "link"
                    }
                },
            },
        ]
    }

    expected = {
        "ordered_cards": [
            {
                "visualization_settings": {
                    "click_behavior": {
                        "parameterMapping": {
                            f"%JSONCONV%[\"dimension\", [\"field\", \"%%{TEST_TABLE_NAME}|{TEST_FIELD_NAME}\", null]]": {
                                "target": {
                                    "type": "dimension",
                                    "id": f"%JSONCONV%[\"dimension\", [\"field\", \"%%{TEST_TABLE_NAME}|{TEST_FIELD_NAME}\", null]]",
                                    "dimension": [
                                        "dimension",
                                        [
                                            "field",
                                            f"%%{TEST_TABLE_NAME}|{TEST_FIELD_NAME}",
                                            None
                                        ]
                                    ]
                                },
                                "id": f"%JSONCONV%[\"dimension\", [\"field\", \"%%{TEST_TABLE_NAME}|{TEST_FIELD_NAME}\", null]]"
                            }
                        },
                        "card_name": f"%targetId%{TEST_CARD_NAME}",
                        "linkType": "question",
                        "type": "link"
                    }
                },
            },
        ]
    }

    api = create_test_api()
    encoded = api.convert_ids2names('', obj, None)
    # print(dumps(encoded, indent=4))
    assert encoded == expected

    decoded = api.convert_names2ids('', '', encoded)
    assert obj == decoded


def test_link_dashboard():
    obj = {
        "ordered_cards": [
            {
                "visualization_settings": {
                    "link": {
                        "entity": {
                            "model": "dashboard",
                            "id": TEST_DASHBOARD_ID,
                        }
                    }
                },
                "dashboard_id": TEST_DASHBOARD_ID,
            },
        ],
    }

    expected = {
        "ordered_cards": [
            {
                "visualization_settings": {
                    "link": {
                        "entity": {
                            "model": "dashboard",
                            "dashboard_name": f"%id%{TEST_DASHBOARD_NAME}",
                        }
                    }
                },
                "dashboard_name": f"%dashboard_id%{TEST_DASHBOARD_NAME}",
            },
        ],
    }

    api = create_test_api()
    encoded = api.convert_ids2names('', obj, None)
    # print(dumps(encoded, indent=4))
    assert encoded == expected

    decoded = api.convert_names2ids('', '', encoded)
    assert obj == decoded
