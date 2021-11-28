import random 
class Data : 
    # Data point object 
    def __init__(self, key, val, version = 0, timestamp = (0,0)) :
        self.key : any = key
        self.val : any  = val
        self.version : int = version
        # Timestamp is a tuple with (Read, Write) timestamp value.
        self.timestamp : tuple(int, int) = timestamp

class Database :
    # Database containing dictionary of data points.
    # Dictionary is choosen such that every data point is assined to a key.
    # Each key will contain the different versions of that data point.
    def __init__(self) :
        self.__data : dict[Data] = {}
    def makeNewVersion(self, key, value) :
        oldData = self.__data[key][-1]
        # ! Check this part 
        self.__data[key] = Data(key, value, oldData.version, oldData.timestamp)
    def generateRandom(self, n : int) :
        # n is the number of random data
        for key in range(n) : 
            val = random.randint(100)
            self.__data[key] = Data(key, val)
    def read(self, transaction, key) :

        # ! Check this 
        return self.__data[key]
    def write(self, transaction, key) :
    
        # ! Check this 
        return self.__data[key]
class Operation :
    def __init__(self, type) :
        self.type : str  = []

class Transaction :
    def __init__(self, timestamp = 0) :
        self.operations : list[Operation] = []
        self.timestamp : int = timestamp

class Schedule :
    def __init__(self) :
        self.schedule : list[Transaction] = []

class App :
    # The app to run the concurrency management process
    def __init__(self) :
        # Timestamp counter that got increased everytime a new timestamp is added
        self.timestamp : int = 0