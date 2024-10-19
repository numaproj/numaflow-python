from threading import Thread


class ThreadWithReturnValue(Thread):
    """
    A custom Thread class that allows the target function to return a value.
    This class extends the built-in threading.Thread class.
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

    def run(self):
        """
        Run the thread.

        This method is overridden from the Thread class.
        It calls the target function and saves the return value.
        """
        if self._target is not None:
            # Execute target and store the result
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args):
        """
        Wait for the thread to complete and return the result.

        This method is overridden from the Thread class.
        It calls the parent class's join() method and then returns the stored return value.

        Parameters:
        *args: Variable length argument list to pass to the join() method.

        Returns:
        The return value from the target function.
        """
        # Call the parent class's join() method to wait for the thread to finish
        Thread.join(self, *args)
        # Return the result of the target function
        return self._return
