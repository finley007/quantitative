#! /usr/bin/env python
# -*- coding:utf8 -*-

class ValidationFailed(Exception):

    def __init__(self, errorinfo):
        super().__init__(self)
        self.errorinfo = errorinfo

    def __str__(self):
        return self.errorinfo

class InvalidStatus(Exception):
    def __init__(self, errorinfo):
        super().__init__(self)
        self.errorinfo = errorinfo

    def __str__(self):
        return self.errorinfo

class InvalidValue(Exception):
    def __init__(self, errorinfo):
        super().__init__(self)
        self.errorinfo = errorinfo

    def __str__(self):
        return self.errorinfo

if __name__ == '__main__':
    try:
        raise ValidationFailed('Data is invalid')
    except ValidationFailed as e:
        print(e)