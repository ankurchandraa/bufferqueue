class ApiResponse(object):

    def __init__(self, success, message):
        self.success = success
        self.message = message

    def get_response(self):
        return self.success, self.message

    def update_success(self, success):
        self.success = success

    def update_message(self, message):
        self.message = message

    def update(self, success, message):
        self.success = success
        self.message = message

    def get_message(self):
        return self.message

    def get_status(self):
        return self.success