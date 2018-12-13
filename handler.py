import sys
import itertools
import time

class treeNode (object):
    def __init__(self,name, support,parentNode):
        """
        Initialize the node.
        """
        self.name = name
        self.support = support
        #self.batch = 0
        self.link = None
        self.parent = parentNode
        self.children = []

    def has_child (self, value):
        """
        check if an item is already in the tree path
        """
        for node in self.children:
            if node.name == value:
                return True
        return False
    def add_child (self, value):
        """
        Add a node as a child.
        """
        child = treeNode(value,1,self)
        self.children.append(child)
        return child
    def get_child (self, value):
        """
        return the node with the a particular key
        """

        for node in self.children:
            if node.name == value:
                return node

        return None

    def increase(self, support):
        """
        increase the support of the node.
        """
        self.support += support

    def display (self,ind=1):
        print('-'*ind,self.name,' ', self.support)
        for child in self.children:
            child.display(ind+1)   


class Tree (object):
    """
    A stream tree.
    """
    def __init__(self, transactions,threshold,fading,root_value,root_count):
        """
        Initialization of the tree
        """
        self.frequent = self.find_frequent(transactions,threshold)
        self.headers = self.build_header_table(self.frequent)
        self.root = self.build_tree(transactions,root_value,root_count,self.frequent,self.headers)
        self.fading = fading

    @staticmethod
    def find_frequent(transactions, threshold):
        """
        create a dictionary with the items that meet the expminsup
        """
        items = {}        
        for transaction in transactions:
            for item in transaction:
                items[item] = items.get(item,0) + 1

        # purge items smaller than the threshold
        for key in list(items.keys()):
            if items[key] < threshold:
                del items[key]
        return items      

    @staticmethod
    def build_header_table(frequent):
        """
        Build the header table.
        """
        headers = {}
        for key in frequent.keys():
            headers[key] = None
        return headers
    
    @staticmethod
    def generate_pattern_list(nodes):
        """
        Generate a list of patterns with support counts
        """
        patterns = {}        
        for item in nodes:
            items = []
            auxDict = {}
            if item.parent.parent == None:
                patterns[tuple(item.name)] = item.support
            else:
                current = item
                suffix = (current.name,)
                
                #items.append(current.name)
                auxDict[current.name] = current.support
                while current.parent.parent is not None:
                    current = current.parent
                    items.append(current.name)
                    auxDict[current.name] = current.support                
                for i in range(1,len(items)+1):
                    for subset in itertools.combinations(items,i):
                        #if item.name in subset:
                            pattern = tuple(sorted(list(subset +suffix)))
                            patterns[pattern] = patterns.get(pattern,0) + min([auxDict[x] for x in subset])         
        return patterns
    
    def update_header_table(self,frequent):
        """
        Update header table with new elements
        """
        for key in frequent.keys():
            if key not in self.headers.keys():
                self.headers[key] = None
    
    def apply_fading (self,node,alfa):
        """
        Apply alfa to all nodes
        """
        for i in node.children:
            i.support *= alfa
            self.apply_fading(i,alfa)
    def insert_transactions(self, transactions, threshold):
        """
        Function to insert new batches.
        """
        tree.apply_fading(tree.root,0.6)

        self.frequent = self.find_frequent(transactions,threshold)
        self.update_header_table(self.frequent)
        if len(self.headers) == 0:
            self.headers = self.build_header_table(self.frequent)
        for transaction in transactions:
            transactionList = [x for x in transaction if x in self.frequent]
            if len(transactionList):
                self.insert_tree(transactionList, self.root, self.headers)

    def build_tree (self, transactions, root_value,root_count,frequent,headers):
        """
        create the tree with the transactions.
        """
        root = treeNode(root_value,root_count,None)
        for transaction in transactions:
            transactionList = [x for x in transaction if x in frequent]
            if len(transactionList):
                self.insert_tree(transactionList, root, headers)
        return root

    def insert_tree(self, items, node, headers):
        """
        insert transaction items into the tree.
        """
        first = items[0]
        child = node.get_child(first)
        if child is not None:
            child.support += 1
        else:
            #add a new children
            child = node.add_child(first)
            if headers[first] is None:
                headers[first] = child
            else:
                current = headers[first]
                while current.link is not None:
                    current = current.link
                current.link = child
        #call the function recursively to add the remain items.
        remaining_items = items[1:]
        if len(remaining_items) > 0:
            self.insert_tree(remaining_items,child,headers)
    ## Mine Functions!!!
    
    def mine_itemsets (self, threshold):
        """
        Mine the frequent itemsets using headers        
        """
        frequent = {}
        items = []
        auxDict = {}
        singletons = self.headers.keys()
        for single in singletons:
            node = self.headers[single]            
            while node is not None:                              
                frequent[tuple(node.name)] = frequent.get(tuple(node.name),0) + node.support
                items.clear()
                auxDict.clear()
                if node.parent.parent is not None:
                    
                #else:
                    current = node
                    suffix = (current.name,)
                    #frequent[tuple(node.name)] = frequent.get(tuple(node.name),0) + node.support
                    auxDict[current.name] = current.support
                    while current.parent.parent is not None:
                        current = current.parent
                        items.append(current.name)
                        auxDict[current.name] = current.support                
                    for i in range(1,len(items)+1):
                        for subset in itertools.combinations(items,i):
                            #if item.name in subset:

                                pattern = tuple(sorted(list(subset +suffix)))
                                frequent[pattern] = frequent.get(pattern,0) + min([auxDict[x] for x in pattern])   
                node = node.link                

        #purge item with minsup smaller than Threshold
        for key in tuple(frequent.keys()):
            if frequent[key] < threshold:
                del frequent[key]
        return frequent
    
    
        

def loadData (data,limit):
    '''
    Load the data into a a list for transactions
    input - datapath
    output - list of transactions
    '''
    result = []
    data = open(data).readlines()
    for transaction in data:
        transaction = transaction.split()[3:]
        result.append(transaction)
    return result[:limit]

def printTransactions(dataset):
    if type(dataset) is list:
        for i in dataset:
            print(i)
    elif type(dataset) is dict:
        keys = sorted(dataset.keys())
        for i in keys:
            print("{} - {:.4f}".format(i,dataset[i]))

BATCH = 50
test = loadData('/Users/dossants/Desktop/DataMining/Project/IBMGenerator-master/Sample100K10A100I.data',100000)
#test = loadData('T10I4D100K.data',6)
batches = [test[i:i + BATCH] for i in range(0, len(test), BATCH)]

start_time = time.time()
tree = Tree([], 1,0.6,'None', 0)
preMinSup = BATCH * 0.002
MinSup = BATCH * 0.02
print("TimeFading Tree")
print("PreMinSup- {}".format(preMinSup))
print("Batch Size - {} ".format(BATCH))


for index in range(len(batches)):
    #printTransactions(batch)
    tree.insert_transactions(batches[index],(preMinSup))
    if ((index+1) % 100) == 0:
        print("{}--- {:.4f} seconds ---".format(index+1, (time.time() - start_time)))
    #tree.root.display()

""" print("Mining Part")
#printTransactions(tree.mine_itemsets(MinSup))
start_time = time.time()
for i in range(1,20,2):
    minsup = i*MinSup
    print("Mined {} - Minsup {} ".format(len(tree.mine_itemsets(minsup)),minsup))
    print("{}--- {} seconds ---".format("Mine minsup=2%", (time.time() - start_time)))
 """
