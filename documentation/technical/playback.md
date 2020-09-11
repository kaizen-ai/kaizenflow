<!--ts-->
   * [Playback](#playback)
      * [Sources](#sources)
      * [Using playback](#using-playback)



<!--te-->

# Playback

- "Playback" is a way to automatically generate a unit test for a given function
  by capturing the inputs applied to the function by the external world

- The working principle is:
   1) instrument the target function `F` to test with a `Playback` object or with
      a decorator `@playback`
   2) run the function `F` using the external code to drive its inputs (e.g.,
      while the function is executed as part of a more complex system, or in a
      notebook)
   3) the playback framework captures the inputs and the output of the function
      `F`, and generates python code to apply the stimuli to `F` and check its
      output to the expected output
   4) modify the automatically generated code to create handcrafted unit tests

## Code and tests

- The code for playback is located at `helpers/playback.py`
- Unit tests for playback with useful usage examples are located at
  `helpers/test/test_playback.py`

## Using playback

### Example 1: testing `get_sum()`

- Assume that we want unit test a function `get_sum()`
  ```python
  def get_sum(a: List[int], b: List[int]) -> Any:
      c = a + b
      return c
  ```

- Assume that typically `get_sum()` gets its inputs from a complex pipeline
  ```python
  def complex_data_pipeline() -> Tuple[List[int], List[int]]:
     # Incredibly complex pipeline generating:
     a = [1, 2, 3]
     b = [4, 5, 6]
     return a, b
  ```

- The function is called with:
   ```
   a, b = complex_data_pipeline()
   c = get_sum(a, b)
   ```

- We don't want to compute by hand the inputs `a, b`, but we can reuse
  `complex_data_pipeline` to create a realistic workload for the function under
  test

- Instrument the code with `Playback`:

  ```python
  def get_sum(a: List[int], b: List[int]) -> Any:
      playback = plbck.Playback("assert_equal", "get_sum", a=a, b=b)
      c = a + b
      code = playback.run(res)
      print(code)
      return c
  ```

- Create the playback object
  ```python
  playback = plbck.Playback("assert_equal", "get_sum", a=a, b=b)
  ```
  which specifies:
  - The unit test mode: "check_string" or "assert_equal"
  - The function name that is being tested: in our case, "get_sum"
  - The function parameters that were created earlier

- Run it with:
    ```python
     a, b = complex_data_pipeline()
     c = get_sum(a, b)
    ```

- Run the playback passing the expected outcome as a parameter
  ```python
  code = playback.run(res)
  ```

- The output `code` will contain a string with the unit test for `get_sum()`

  ```python
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
