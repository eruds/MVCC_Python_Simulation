import random 
import threading 
import math
from typing import List, NewType

# Type Definitions 
# Instruction = NewType("Instruction", tuple(str, int, int))
# Timestamp = tuple(int,int)

class Data : 
    # Data point object 
    def __init__(self, key, val, version = 0, timestamp = (0,0)) :
        # To simplify things, value can only be a number.
        self.key : any = key
        self.val : int  = val
        self.version : int = version
        # Timestamp is a tuple with (Read, Write) timestamp value.
        self.timestamp : tuple(str, int, int) = timestamp
    def __str__(self) :
        str = "val : " + self.val + "timestamp : " + self.timestamp + " version : " + self.version 
        return str

class Database :
    # Database containing dictionary of data points.
    # Dictionary is choosen such that every data point is assined to a key.
    # Each key will contain the different versions of that data point.
    # Key 0 is reserve for a null value 
    def __init__(self) :
        self.__data : dict[Data] = {}
        self.__log : list[str] = []

    def __len__(self) :
        return len(self.__data)
        
    def print (self) :
        # Print the database to the CLI 
        for key, value in self.__data :
            print(f"{key} : {value}")
    
    
    def printLog(self) :
        # Print the log, which consist of all of the instruction being executed and the transaction executing them.
        print(self.__log)

    def addLog(self) :
        self.__log.append(())

    def makeNewVersion(self, key, value) :
        # Make a new version of a data point
        oldData = self.__data[key][-1]
        # ! Check this part 
        self.__data[key] = Data(key, value, oldData.version, oldData.timestamp)

    def generateRandom(self, n : int) :
        # Generate a random data for the database. 
        # n is the number of random data
        for key in range(1, n) : 
            val = random.randint(0, 100)
            self.__data[key] = Data(key, val)
    
    def read(self, key, timestamp) :
        # Read a datapoint using the MVCC rule
        # ! Check this 
        return self.__data[key]
    def write(self, key, val, timestamp) :
        # Write a datapoint using the MVCC rule
    
        # ! Check this 
        return self.__data[key]


class Transaction :
    def __init__(self, id : int, timestamp : int = 0, instructions : "list[tuple(str, int, int)]" = 0) :
        # An instruction is a tuple of (operation, target datapoint)
        # An operation is a string consisting of Read, Write, Commit, Rollback.
        # Another set of operation is Add, Subtract, Multiply, Divide
        # Except for Add, Subtract, Multiply and Divide, the second key can be left as 0 to indicate a null value. 
        #! The second key is reserved for a number, not a key, to simplify things 
        self.id : int = id
        self.instructions : list[tuple(str, int, int)] = instructions
        self.timestamp : int = timestamp
        # Start, commited, rolledback, idk   #!checkthis.
        self.status : str = "start"
        self.cache : dict = {}
    
    def execute(self, database : Database, instruction : "tuple(str, int, int)") :
        operation = instruction[0]
        key = instruction[1]
        num = instruction[2]
        if(operation == "read") :
            self.cache[key] = database.read(key, self.timestamp)
            print(f"[Transaction {self.id}] Datapoint [{key}] is added to the cache")
        elif(operation == "write") :
            database.write(key, self.cache[key], self.timestamp)
            print(f"[Transaction {self.id}] Datapoint [{key}] with value {self.cache[key]}")
        elif(operation == "add") :
            print(f"[Transaction {self.id}] Add {self.cache[key]} by {num} in cache")
            self.cache[key] = self.cache[key] + num 
        elif(operation == "subtract") :
            print(f"[Transaction {self.id}] Subtract {self.cache[key]} by {num} in cache")
            self.cache[key] = self.cache[key] - num 
        elif(operation == "multiply") :
            print(f"[Transaction {self.id}] Multiply {self.cache[key]} by {num} in cache")
            self.cache[key] = self.cache[key] * num 
        elif(operation == "divide") :
            # Division is always the floor rounding 
            print(f"[Transaction {self.id}] Divide {self.cache[key]} by {num} in cache")
            self.cache[key] = math.floor(self.cache[key] / num )
        elif(operation == "commit" ) :
            print(f"[Transaction {self.id}] COMITTED")
        elif(operation == "rollback") :
            print(f"[Transaction {self.id}] ROLLED BACK")
        else : 
            print(f"[Transaction {self.id}] Illegal/Unknown operation")

    def run(self, database : Database) :
        for instruction in self.instructions :
            self.execute(database, instruction)
        

class App :
    # The app to run the concurrency management process
    def __init__(self) :
        # Timestamp counter that got increased everytime a new timestamp is added
        self.__timestamp : int = 0
        self.schedule : list[Transaction] = []
        self.database : Database = Database()
        # All the possible operation
        self.__operations : list[str] = ["read", "write", "add", "subtract", "multiply", "divide", "commit", "rollback"]

    def printSchedule(self) :
        print("Schedule")
        print("--------------------------")
        for transaction in self.schedule :
            print(f"[Transaction {transaction.id}] {transaction.instructions}")

    def generateTransactions(self, n : int = 1) :
        # Generate a random transaction 
        def generateRandomInstructions(self, n : int, m : int) :
            #  n = number of data in the database
            #  m = number of instruction to generate 
            instructions = []
            for i in range(m) :
                randN = random.randint(1, n)
                randM = random.randint(1, len(self.__operations)-1)
                instruction = (randN, self.__operations[randM])
                instructions.append(instruction)
            return instructions
        transaction = Transaction(self.__timestamp, self.__timestamp, generateRandomInstructions(self, len(self.database)-1, 8)) 
        self.schedule.add(transaction)

    def run(self) :
        print("Starting the application.")
        # Initialize a random database 
        self.database.generateRandom(10)
        print("Database : ")
        self.database.print()
        # Initialize a random list of transactions
        self.generateTransactions() 
        self.printSchedule()
        # for transaction in self.schedule :
        #     print("[START] Transaction", transaction.id)
        #     thread = threading.Thread(target=transaction.run, args=[self.database])
        #     thread.start()


app = App()
app.run()