from pyzero_dtq.application import Application


class AppStub(Application):
    '''A application stub that the collector will pass on to workers at worker stating time.
    '''

    def accept(self, criteria):
        '''By returning True, the app tells the worker that the task is a category of its interest for later processing
        '''
        return True

    def run(self, task):
        '''All that is done here is to return the same that was received
        '''
        return task
