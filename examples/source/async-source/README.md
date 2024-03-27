# Example Python User Defined Source
A simple example of a user-defined source. The source maintains an array of messages and implements the `Read`, 
`Ack`, and `Pending` methods:
- The `Read(x)` method returns the next `x` number of messages in the array. 
- The `Ack()` method acknowledges the last batch of messages returned by Read(). 
- The `Pending()` method returns 0 to indicate that the simple source always has 0 pending messages.