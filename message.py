class Message():
    def __init__(self, message, **kwargs):
        self.message = message
        self.params = dict()
        for k in kwargs:
            self.params[k] = kwargs[k]
    
    def __str__(self):
        x = f'Message: {self.message}, Params = '
        x += '{'
        if len(self.params) > 0:
            for p in self.params:
                x += f'{p} : {self.params[p]}, '
            x = x[:-2] + '}'
        else:
            x += 'None}'
        return  x
