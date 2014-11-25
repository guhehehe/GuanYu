# -*- encoding: utf-8 -*-


class Error(Exception):
    def __str__(self):
        return self.message

class TooManyTasksError(Error):
    def __init__(self):
        self.message = 'Too many tasks are assigned to workers.'
