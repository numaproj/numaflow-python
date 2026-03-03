from threading import Thread


class ThreadWithReturnValue(Thread):
    """
    A custom Thread class that allows the target function to return a value.
    This class extends the built-in threading.Thread class.
    Exceptions raised by the target are captured and re-raised on join().
    """

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, verbose=None):
        """
        Initializes the ThreadWithReturnValue object.

        Parameters:
        group (threading.ThreadGroup)
        target (callable): The callable object to be invoked by the run() method. Defaults to None.
        name (str): The thread name. Defaults to None.
        args (tuple): The argument tuple for the target invocation. Defaults to ().
        kwargs (dict): The dictionary of keyword arguments for the target invocation.
         Defaults to {}.
        verbose: Not used, defaults to None.
        """
        Thread.__init__(self, group, target, name, args, kwargs)
        # Variable to store the return value of the target function
        self._return = None
        self._exception: BaseException | None = None

    def run(self):
        """
        Run the thread.

        This method is overridden from the Thread class.
        It calls the target function and saves the return value.
        If the target raises, the exception is captured for re-raising on join().
        """
        if self._target is not None:
            try:
                # Execute target and store the result
                self._return = self._target(*self._args, **self._kwargs)
            except BaseException as exc:
                self._exception = exc

    def join(self, *args):
        """
        Wait for the thread to complete and return the result.

        This method is overridden from the Thread class.
        It calls the parent class's join() method, re-raises any captured
        exception, and then returns the stored return value.

        Parameters:
        *args: Variable length argument list to pass to the join() method.

        Returns:
        The return value from the target function.

        Raises:
        BaseException: If the target function raised during run().
        """
        # Call the parent class's join() method to wait for the thread to finish
        Thread.join(self, *args)
        if self._exception is not None:
            raise self._exception
        # Return the result of the target function
        return self._return
