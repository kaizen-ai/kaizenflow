import helpers.hunit_test as hunit_test
import unittest.mock as umock
import helpers.hio as hio
import data_schema.dataset_schema_utils as dsdascut
import copy

DUMMY_SCHEMA = {
    "dataset_signature":
    "download_mode.downloading_entity.action_tag",
    "token_separator_character": ".",
    "allowed_values": {
        "download_mode": ["bulk"],
        "downloading_entity": ["airflow"],
        "action_tag": ["downloaded_1sec"]
    }
}

class TestGetDatasetSchema1(hunit_test.TestCase):
    
    @umock.patch.object(dsdascut, "_get_dataset_schema_file_path")
    @umock.patch.object(hio, "from_json")
    def test_get_dataset_schema1(
        self, 
        mock_from_json: umock.MagicMock,  
        mock_get_dataset_schema_file_path: umock.MagicMock
    ) -> None:
        """
        Verify that dataset schema is loaded correctly
        """
        test_schema = copy.deepcopy(DUMMY_SCHEMA)
        mock_from_json.return_value = copy.deepcopy(DUMMY_SCHEMA)
        mock_get_dataset_schema_file_path.return_value = "dataset_schema_v3.json"
        expected_value = test_schema
        expected_value["version"] = "v3"
        actual_value = dsdascut.get_dataset_schema()
        self.assertDictEqual(expected_value, actual_value)
        
    
class TestValidateDatasetSignatureSyntax1(hunit_test.TestCase):
    def test_validate_dataset_signature_syntax_valid1(self) -> None:
        """
        Assure that valid signature passes the syntax check.
        """
        # This signature is valid according to the DUMMY_SCHEMA.
        valid_signature = "bulk.airflow.downloaded_1sec"
        self.assertTrue(dsdascut._validate_dataset_signature_syntax(valid_signature, DUMMY_SCHEMA))
    
    def test_validate_dataset_signature_syntax_invalid1(self) -> None:
        """
        Assure that invalid signature fails the syntax check.
        """
        # This signature is invalid according to the DUMMY_SCHEMA.
        invalid_signature = "airflow.downloaded_1sec"
        self.assertFalse(dsdascut._validate_dataset_signature_syntax(invalid_signature, DUMMY_SCHEMA))
        
class TestValidateDatasetSignatureSemantics1(hunit_test.TestCase):
    def test_validate_dataset_signature_semantics_valid1(self) -> None:
        """
        Assure that valid signature passes the semantics check.
        """
        # This signature is valid according to the DUMMY_SCHEMA.
        valid_signature = "bulk.airflow.downloaded_1sec"
        self.assertTrue(dsdascut._validate_dataset_signature_semantics(valid_signature, DUMMY_SCHEMA))
    
    def test_validate_dataset_signature_semantics_invalid1(self) -> None:
        """
        Assure that invalid signature fails the semantics check.
        """
        # This signature is invalid according to the DUMMY_SCHEMA.
        invalid_signature = "bulk.airflow.downloaded_48sec"
        self.assertFalse(dsdascut._validate_dataset_signature_semantics(invalid_signature, DUMMY_SCHEMA))
        
class TestValidateDatasetSignature1(hunit_test.TestCase):
    def test_validate_dataset_signature_valid1(self) -> None:
        """
        Assure that valid signature passes the validation.
        """
        # This signature is valid according to the DUMMY_SCHEMA.
        valid_signature = "bulk.airflow.downloaded_1sec"
        self.assertTrue(dsdascut.validate_dataset_signature(valid_signature, DUMMY_SCHEMA))
    
    def test_validate_dataset_signature_invalid1(self) -> None:
        """
        Assure that syntactically invalid signature fails the validation.
        """
        # This signature is invalid according to the DUMMY_SCHEMA.
        invalid_signature = "bulk.downloaded_48sec"
        self.assertFalse(dsdascut.validate_dataset_signature(invalid_signature, DUMMY_SCHEMA))
        
    def test_validate_dataset_signature_invalid2(self) -> None:
        """
        Assure that semantically invalid signature fails the validation.
        """
        # This signature is invalid according to the DUMMY_SCHEMA.
        invalid_signature = "madeup.airflow.downloaded_1sec"
        self.assertFalse(dsdascut.validate_dataset_signature(invalid_signature, DUMMY_SCHEMA))