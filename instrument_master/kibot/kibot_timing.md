# Kibot timing

- [gdoc](https://docs.google.com/document/d/1BdOj3DGpFzHQZ6dpYCMMAeyjTtqYgltyqDbQ7n8Vde8/edit#)

- Kibot documentation (from http://www.kibot.com/Support.aspx#data_format)
  states the following timing semantic:

  - "A time stamp of 10:00 AM is for a period between 10:00:00 AM and 10:00:59
    AM"
  - "All records with a time stamp between 9:30:00 AM and 3:59:59 PM represent
    the regular US stock market trading session."

- Thus the open price at time "ts" corresponds to the instantaneous price at
  time "ts", which by our conventions corresponds to the "end" of an interval in
  the form [a, b) interval

- As a consequence our usual "ret_0" # (price entering instantaneously at time t
    - 1 and exiting at time t) is implemented in terms of Kibot data as:

    ret_0(t) = open_price(t) - open_price(t - 1)

  ```text
               datetime     open     high      low    close   vol      time  ret_0
  0 2009-09-27 18:00:00  1042.25  1043.25  1042.25  1043.00  1354  18:00:00    NaN
  1 2009-09-27 18:01:00  1043.25  1043.50  1042.75  1042.75   778  18:01:00   1.00
  ```

- E.g., ret_0(18:01) is the return realized entering (instantaneously) at 18:00
  and exiting at 18:01

- In reality we need time to:
    - Compute the forecast
    - Enter the position
- We can't use open at time t - 1 since this would imply instantaneous forecast
- We can use data at time t - 2, which corresponds to [t-1, t-2), although still
  we would need to enter instantaneously
- A better assumption is to let 1 minute to enter in position, so:
    - Use data for [t - 2, t - 1) (which Kibot tags with t - 2)
    - Enter in position between [t - 1, t)
    - Capture the return realized between [t, t + 1]
- In other terms we need 1 extra delay (probably 2 would be even safer)
