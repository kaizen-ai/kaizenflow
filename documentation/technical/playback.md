<!--ts-->
   * [Playback](#playback)
      * [Sources](#sources)
      * [Using playback](#using-playback)



<!--te-->

# Playback

- "Playback" is a way to automatically generate a unit test for a given
  function.

## Sources

- The code for playback is located at `helpers/playback.py`
- Unit tests for playback with useful usage examples are located at
  `helpers/test/test_playback.py`

## Using playback

- **Example 1**: running playback to unit test a function `get_sum()`

  ```python
  def get_sum(a: Any, b: Any) -> Any:
      return a + b
  ```
  - Create the parameters for the function
    ```python
    a = [1, 2, 3]
    b = [4, 5, 6]
    ```
  - Create the playback object, specifying
    - The unit test mode: "check_string" or "assert_equal"
    - The function name: in our case, "get_sum"
    - The function parameters that were created earlier
    ```python
    playback = plbck.Playback("assert_equal", "get_sum", a=a, b=b)
    ```
  - Run the function with the created parameters to get the actual result
    ```python
    res = get_sum(a, b)
    ```
  - Run the playback passing the actual result as a parameter
    ```python
    code = playback.run(res)
    ```
  - The output `code` will contain a string with the unit test for `get_sum()`

    ```
    import helpers.unit_test as hut

    class TestGetSum(hut.TestCase):
        def test1(self) -> None:
            # Initialize function parameters.
            a = [1, 2, 3]
            b = [4, 5, 6]
            # Get the actual function output.
            act = get_sum(a, b)
            # Create the expected function output.
            exp = [1, 2, 3, 4, 5, 6]
            # Check whether the expected value equals the actual value.
            self.assertEqual(act, exp)
    ```

- **Example 2**: running playback to unit test a function `get_dict_union()`

  ```python
  def get_dict_union(dict1: Dict[Any, Any], dict2: Dict[Any, Any]) -> Any:
      dict_union = {}
      dict_union.update(dict1)
      dict_union.update(dict2)
      return dict_union
  ```
  - Playback supports complicated data structures with recursion
    ```python
    dict_with_list = {"1": ["a", 2]}
    dict_with_df = {"3": pd.DataFrame({"Price": [700, 250, 800, 1200]}), "4": {"5": 6}}
    playback = plbck.Playback("assert_equal", "get_dict_union", dict1=dict_with_list, dict2=dict_with_df)
    res = get_dict_union(a, b)
    code = playback.run(res)
    ```
  - The generated unit test is stored as string in `code`

    ```python
    import helpers.unit_test as hut

    class TestGetDictUnion(hut.TestCase):
        def test1(self) -> None:
            # Initialize function parameters.
            dict1 = {"1": ["a", 2]}
            dict2 = {"3": pd.DataFrame.from_dict({'Price': [700, 250, 800, 1200]}), "4": {"5": 6}}
            # Get the actual function output.
            act = get_dict_union(dict1, dict2)
            # Create the expected function output.
            exp = {"1": ["a", 2], "3": pd.DataFrame.from_dict({'Price': [700, 250, 800, 1200]}), "4": {"5": 6}}
            # Check whether the expected value equals the actual value.
            self.assertEqual(act, exp)
    ```