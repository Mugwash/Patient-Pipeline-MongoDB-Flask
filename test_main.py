import unittest
from unittest.mock import patch
from git import Repo
import pandas as pd
import pytest
from main import clone_repo, insert_data, pipeline_setup, transform_data, raw_collection, transformed_collection
import asyncio

class TestMain(unittest.TestCase):
    @patch('main.Repo.clone_from')
    def test_clone_repo_failure(self, mock_clone_from):
    # Arrange
        mock_clone_from.side_effect = Exception("Failed to clone repo")

    # Act & Assert
        with self.assertRaises(Exception) as context:
            asyncio.run(clone_repo())
        self.assertTrue('Failed to clone repo' in str(context.exception))
    
    @patch('main.Repo.clone_from')
    def test_clone_repo_success(self, mock_clone_from):
        # Arrange
        mock_clone_from.return_value = None

        # Act
        with patch('builtins.print') as mocked_print:
            asyncio.run(clone_repo())

        # Assert
        mocked_print.assert_called_once_with("Repo cloned successfully")
    @patch('main.insert_data')
    async def test_insert_data_success(self, mock_listdir, mock_read_json, mock_loads, mock_insert_many):
        # Arrange
        mock_listdir.return_value = ["data1.json", "data2.json"]
        mock_read_json.return_value = pd.DataFrame({"key": ["value"]})
        mock_loads.return_value = [{"key": "value"}]

        # Act
        await insert_data()

        # Assert
        mock_listdir.assert_called_once_with("/app/data")
        mock_read_json.assert_called_once_with("/app/data/data1.json")
        mock_loads.assert_called_once_with(pd.DataFrame({"key": ["value"]}).T.to_json())
        mock_insert_many.assert_called_once_with([{"key": "value"}])
        self.assertTrue(mock_insert_many.return_value.acknowledged)
        self.assertEqual(mock_insert_many.return_value.inserted_ids, None)
        self.assertEqual(mock_insert_many.return_value.inserted_count, 1)
        self.assertEqual(mock_insert_many.return_value.acknowledged, True)

    @patch('main.Repo.clone_from')
    def test_clone_repo_failure(self, mock_clone_from):
        # Arrange
        mock_clone_from.side_effect = Exception("Failed to clone repo")

        # Act & Assert
        with self.assertRaises(Exception) as context:
            asyncio.run(clone_repo())
        self.assertTrue('Failed to clone repo' in str(context.exception))

    @patch('main.Repo.clone_from')
    def test_clone_repo_success(self, mock_clone_from):
        # Arrange
        mock_clone_from.return_value = None

        # Act
        with patch('builtins.print') as mocked_print:
            asyncio.run(clone_repo())

        # Assert
        mocked_print.assert_called_once_with("Repo cloned successfully")

    @patch('main.raw_collection.insert_many')
    @patch('main.json.loads')
    @patch('main.pd.read_json')
    @patch('main.os.listdir')
    @patch('main.insert_data')
    async def test_insert_data_success(self, mock_listdir, mock_read_json, mock_loads, mock_insert_many):
        # Arrange
        mock_listdir.return_value = ["data1.json", "data2.json"]
        mock_read_json.return_value = pd.DataFrame({"key": ["value"]})
        mock_loads.return_value = [{"key": "value"}]

        # Act
        await insert_data()

        # Assert
        mock_listdir.assert_called_once_with("/app/data")
        mock_read_json.assert_called_once_with("/app/data/data1.json")
        mock_loads.assert_called_once_with(pd.DataFrame({"key": ["value"]}).T.to_json())
        mock_insert_many.assert_called_once_with([{"key": "value"}])
        self.assertTrue(mock_insert_many.return_value.acknowledged)
        self.assertEqual(mock_insert_many.return_value.inserted_ids, None)
        self.assertEqual(mock_insert_many.return_value.inserted_count, 1)
        self.assertEqual(mock_insert_many.return_value.acknowledged, True)

    @patch('main.clone_repo')
    @patch('main.insert_data')
    @patch('main.transform_data')
    @patch('builtins.print')
    async def test_pipeline_setup_success(self, mock_print, mock_transform_data, mock_insert_data, mock_clone_repo):
        # Arrange

        # Act
        asyncio.run(pipeline_setup())

        # Assert
        mock_clone_repo.assert_awaited_once()
        mock_insert_data.assert_awaited_once()
        mock_transform_data.assert_awaited_once()
        mock_print.assert_called_once_with("Repo cloned, data inserted and transformed successfully")

    @patch('main.transformed_collection.insert_many')
    @patch('main.pd.DataFrame')
    @patch('main.raw_collection.find')
    async def test_transform_data_success(self, mock_find, mock_dataframe, mock_insert_many):
        # Arrange
        mock_find.return_value = [
            {
                '_id': 1,
                'entry': {
                    'resource': {
                        'resourceType': 'type1',
                        'billablePeriod': {
                            'start': '2022-01-01',
                            'end': '2022-01-31'
                        },
                        'insurance': 'insurance1',
                        'patient': 'patient1',
                        'status': 'status1'
                    }
                }
            },
            {
                '_id': 2,
                'entry': {
                    'resource': {
                        'resourceType': 'type2',
                        'billablePeriod': {
                            'start': '2022-02-01',
                            'end': '2022-02-28'
                        },
                        'insurance': 'insurance2',
                        'patient': 'patient2',
                        'status': 'status2'
                    }
                }
            }
        ]
        mock_dataframe_instance = mock_dataframe.return_value
        mock_dataframe_instance.__getitem__.side_effect = lambda key: mock_dataframe_instance if key == '_id' else None
        mock_dataframe_instance.to_dict.return_value = [
            {
                '_id': 1,
                'type': None,
                'resourceType': 'type1',
                'resource': {
                    'resourceType': 'type1',
                    'billablePeriod_start': '2022-01-01',
                    'billablePeriod_end': '2022-01-31',
                    'insurance': 'insurance1',
                    'patient': 'patient1',
                    'status': 'status1'
                }
            },
            {
                '_id': 2,
                'type': None,
                'resourceType': 'type2',
                'resource': {
                    'resourceType': 'type2',
                    'billablePeriod_start': '2022-02-01',
                    'billablePeriod_end': '2022-02-28',
                    'insurance': 'insurance2',
                    'patient': 'patient2',
                    'status': 'status2'
                }
            }
        ]

        # Act
        result = await transform_data()

        # Assert
        mock_find.assert_called_once()
        mock_dataframe.assert_called_once_with(mock_find.return_value)
        mock_dataframe_instance.__getitem__.assert_called_once_with('_id')
        mock_dataframe_instance.drop.assert_called_once_with('entry', axis=1)
        mock_insert_many.assert_called_once_with(mock_dataframe_instance.to_dict.return_value)
        self.assertEqual(result, None)
        self.assertEqual(mock_dataframe_instance.to_dict.call_count, 1)
        self.assertEqual(mock_dataframe_instance.drop.call_count, 1)
        self.assertEqual(mock_insert_many.call_count, 1)
        self.assertEqual(mock_insert_many.return_value.acknowledged, True)
        self.assertEqual(mock_insert_many.return_value.inserted_ids, None)
        self.assertEqual(mock_insert_many.return_value.inserted_count, 2)
        self.assertEqual(mock_insert_many.return_value.acknowledged, True)
        self.assertEqual(mock_dataframe_instance.to_dict.call_args_list, [unittest.mock.call('records')])
        self.assertEqual(mock_insert_many.call_args_list, [unittest.mock.call(mock_dataframe_instance.to_dict.return_value)])
        self.assertEqual(mock_dataframe_instance.drop.call_args_list, [unittest.mock.call('entry', axis=1)])

if __name__ == '__main__':
    unittest.main()