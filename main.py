import random 
import threading 
import math
from typing import List, NewType

# Type Definitions 
# Instruction = NewType("Instruction", tuple(str, int, int))
# Timestamp = tuple(int,int)

# Define a rollback error to raise if there is a rollback
class RollbackError(Exception) :
    # To signify that a transaction need to be rolledback
    pass

class RestartError(Exception) :
    # To signify that an error has occured and the transaction need to be restarted
    pass


# A Datapoint object 
class Data : 
    # Data point object 
    def __init__(self, key, val, version = 0, timestamp = [0,0]) :
        # To simplify things, value can only be a number.
        self.key : any = key
        self.val : int  = val
        self.version : int = version
        # Timestamp is a list with (Read, Write) timestamp value.
        self.timestamp : list(int, int) = timestamp
    def __str__(self) :
        string = "val : " + str(self.val) + " timestamp : " + str(self.timestamp) + " version : " + str(self.version) 
        return string

# Database Object to store data and allow/process data manipulation 
class Database :
    # Database containing dictionary of data points.
    # Dictionary is choosen such that every data point is assined to a key.
    # Each key will contain the different versions of that data point.
    # Key 0 is reserve for a null value 
    def __init__(self) :
        self.__data : dict[list[Data]] = {}
        self.__log : list[str] = []
        self.__lock = threading.Lock()

    def __len__(self) :
        return len(self.__data)
        
    def print (self) :
        # Print the database to the CLI 
        for key in self.__data :
            print(f"{key} : ",end="")
            for data in self.__data[key] : 
                print(f"| {data} |", end="")
            print()
        
    
    def printLog(self) :
        # Print the log, which consist of all of the instruction being executed and the transaction executing them.
        print(self.__log)

    def addLog(self) :
        self.__log.append(())

    def addNewVersion(self, key, value, timestamp) :
        # Make a new version of a data point
        oldData = self.__data[key][-1]
        newData = Data(key, value, oldData.version+1, [timestamp, timestamp])
        self.__data[key].append(newData)
    
    def getVersion(self, key, timestamp) : 
        data = self.__data[key]
        maxData = data[0]
        for version in data :
            if(version.timestamp[1] > timestamp) :
                continue
            maxData = maxData if maxData.timestamp[1] > version.timestamp[1] else version
        return maxData            

    def generateRandom(self, n : int) :
        # Generate a random data for the database. 
        # n is the number of random data
        for key in range(1, n+1) : 
            val = random.randint(0, 50)
            self.__data[key] = [Data(key, val)]
    
    def read(self, key, transaction) :
        # Read a datapoint using the MVCC rule
        data = self.getVersion(key, transaction.timestamp)
        if(data.timestamp[0] < transaction.timestamp) :
            data.timestamp[0] = transaction.timestamp
        return data.val
    def write(self, key, val, transaction) :
        # Write a datapoint using the MVCC rule
        data = self.getVersion(key, transaction.timestamp)
        if(data.timestamp[0] > transaction.timestamp) :
            raise RollbackError
        elif(data.timestamp[1] == transaction.timestamp ) :
            data.val = val
        else :
            self.addNewVersion(key, val, transaction.timestamp)

# Define a transaction 
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
        # start, active, rollback, commited
        self.__status : str = "start"
        self.__cache : dict = {}
    
    def __str__(self) :
        string = "id : " + str(self.id) + " timestamp : " + str(self.timestamp)
        return string 

    def getStatus(self) :
        return self.__status

    def setStatus(self, status):
        self.__status = status
    
    def rollback(self) :
        self.__status = "rollback"
        print(f"[Transaction {self.id}][ROLLED BACK]")
       
    
    def execute(self, database : Database, instruction : "tuple(str, int, int)") :
        operation = instruction[0]
        key = instruction[1]
        num = instruction[2]
        if(operation == "read") :
            self.__cache[key] = database.read(key, self)
            print(f"[Transaction {self.id}][Read] Datapoint [{key}] is added to the cache")
        elif(operation == "write") :
            try : 
                database.write(key, self.__cache[key], self)
                print(f"[Transaction {self.id}][Write] Datapoint [{key}] with the value {self.__cache[key]}")
                # lock = threading.Lock()
                # with lock : 
                #     print("Database : ")
                #     database.print()
            except RollbackError :
                self.rollback()
                return
        elif(operation == "add") :
            print(f"[Transaction {self.id}][Add] Datapoint [{key}] {self.__cache[key]} by {num} in cache")
            self.__cache[key] = self.__cache[key] + num 
        elif(operation == "subtract") :
            print(f"[Transaction {self.id}][Subtract] Datapoint [{key}] {self.__cache[key]} by {num} in cache")
            self.__cache[key] = self.__cache[key] - num 
        elif(operation == "multiply") :
            print(f"[Transaction {self.id}][Multiply] Datapoint [{key}] {self.__cache[key]} by {num} in cache")
            self.__cache[key] = self.__cache[key] * num 
        elif(operation == "divide") :
            # Division is always the floor rounding 
            print(f"[Transaction {self.id}][Divide] Datapoint [{key}] {self.__cache[key]} by {num} in cache")
            self.__cache[key] = math.floor(self.__cache[key] / num )
        elif(operation == "commit" ) :
            self.__status = "commited"
            print(f"[Transaction {self.id}][COMITTED]")
            lock = threading.Lock()
            with lock :
                print(f"[Transaction {self.id}] Database : ")
                database.print()
        else : 
            print(f"[Transaction {self.id}] Illegal/Unknown operation")

    def run(self, database : Database) :
        for instruction in self.instructions :
            if(self.__status == "rollback") :
                print(f"[Transaction {self.id}][Restarting the transaction...]")
                self.__status = "start"
                break
            self.execute(database, instruction)
        
# Main app that controls the concurrency process. 
class App :
    # The app to run the concurrency management process
    def __init__(self) :
        # Timestamp counter that got increased everytime a new timestamp is added
        self.__timestamp : int = 0
        self.schedule : list[Transaction] = []
        self.database : Database = Database()
        # All the possible operation
        self.__operations : list[str] = ["read", "write", "add", "subtract", "multiply", "divide", "commit"]
        self.countRollback = 0

    def printSchedule(self) :
        print("Schedule")
        print("--------------------------")
        for transaction in self.schedule :
            print(f"[Transaction {transaction.id}]: ")
            i = 1 
            for instruction in transaction.instructions :
                if ( i % 3 != 0  ) : 
                    print(instruction, end=" ")
                else :
                    print(instruction)
                i += 1
            print()

    def generateTransactions(self, n : int = 1) :
        # Generate a random transaction 
        def generateRandomInstructions(self, n : int, m : int) :
            #  n = number of data in the database
            #  m = number of instruction to generate 
            instructions = []
            cache = []
            for i in range(m) :
                randN = random.randint(1, n)
                secondVal = 0
                # Just to make the possibility of a write operation bigger
                temp = self.__operations
                randM = random.randint(1, len(temp)-1)
                operation = temp[randM]
                if(operation != "read" and randN not in cache) :
                    operation = "read"
                    cache.append(randN)
                if(operation not in ["read", "write", "commit"]) :
                    secondVal = random.randint(1, 100)
                if(operation == "commit") : 
                    randN = 0
                if(operation == "commit" and i < math.floor(m/2)) :
                    # if(cache in range())
                    operation = "read"
                    randN = random.randint(1, n)
                    # while randN in cache : 
                    #     randN = random.randint(1, n)
                if(operation == "commit" and i > math.floor(m/2)) :
                    operation = "write"
                    randN = random.choice(cache)
                instruction = (operation, randN, secondVal)
                instructions.append(instruction)
                if(operation == "commit") :
                    if(len(instructions) == 1) : 
                        instructions.pop(0)
                        continue 
                    break
            if(instructions[-1][0] != "commit") : 
                instructions.append(("commit", 0,0))
            return instructions
        for id in range(1,n+1) :
            transaction = Transaction(id, self.__timestamp, generateRandomInstructions(self, len(self.database), 9)) 
            self.schedule.append(transaction)

    def run(self) :
        print("Starting the application.")
        # Initialize a random database 
        self.database.generateRandom(3)
        print("Database : ")
        self.database.print()
        # Initialize a random list of transactions
        self.generateTransactions(4) 
        self.printSchedule()
        print("[START] Start Schedule")
        print("--------------------------")
        while True :
            if threading.active_count()-1 == len(self.schedule) :
                continue
            for transaction in self.schedule :
                if(transaction.getStatus() == "start") : 
                    print("[START] Transaction", transaction.id)
                    thread = threading.Thread(target=transaction.run, args=[self.database])
                    self.__timestamp +=1
                    transaction.timestamp = self.__timestamp
                     
                    transaction.setStatus("active")
                    thread.start()
            if threading.active_count()-1 == 0 : 
                break


app = App()
app.run()