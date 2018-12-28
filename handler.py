import sys
import itertools
import time
import os
from  multiprocessing.pool import Pool
import concurrent.futures
import getopt
import gc
import faulthandler

class treeNode (object):
    def __init__(self,name, support,parentNode):
        """
        Initialize the node.
        """
        self.name = name
        self.support = support
        self.batch = 0
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
        print('-'*ind,self.name,' ', self.support,' ',self.batch)
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
        self.minsup = 0 # this attribute is going to be set on the mining time.

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
    
    def generate_pattern_list(self, nodes):
        """
        Generate a list of patterns with support counts
        """
        patterns = {}        
        for item in nodes:
            items = []
            auxDict = {}
            if item.parent.parent == None:
                patterns[tuple(item.name)] = self.update_support(item,True) #item.support
            else:
                current = item
                suffix = (current.name,)                
                auxDict[current.name] = self.update_support(item,True) #current.support
                while current.parent.parent is not None:
                    current = current.parent
                    items.append(current.name)
                    auxDict[current.name] = self.update_support(item,True) #current.support                
                for i in range(1,len(items)+1):
                    for subset in itertools.combinations(items,i):
                        #if item.name in subset:
                            pattern = tuple(sorted(list(subset +suffix)))
                            patterns[pattern] = patterns.get(pattern,0) + min([auxDict[x] for x in subset])         
                            #Verify, this code maybe improved using the suffix support instead of finding the smaller one.
        return patterns
    
    def update_support(self,node, ismining):
        """
        Compare the batch number between the node and the root.
        Update the value accordingly.
        """
        if ismining:
            sup = 0
            rBatch = self.root.batch -1
        else:
            sup = 1
            rBatch = self.root.batch
        if node.batch == rBatch:
            node.support += sup
        else:
            """ for i in range((rBatch - node.batch)):
                node.support *= self.fading """
            node.support *= pow(self.fading,(rBatch - node.batch)) 
            node.support += sup
            node.batch = rBatch
        return node.support
    
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
        #self.apply_fading(self.root,0.6)

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
            #child.support += 1
            self.update_support(child,False)
        else:
            #add a new children
            child = node.add_child(first)
            child.batch = self.root.batch
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
    def clean_singleton(self, threshold):
        """
        purge singletons which does not met Minsup Criteria                
        """
        result = list()
        purged = list()
        for key in self.headers.keys():
            sup = 0
            node = self.headers[key]
            sup += node.support
            while node.link is not None:
                node = node.link
                sup += node.support
            if sup >= threshold:
                result.append(key)
            else:
                purged.append(key)
        if len(purged) > 0:
            print(purged)
        return result , purged

    def mine_itemsets_thread (self, threshold):
        self.minsup = threshold
        singletons, purged = self.clean_singleton(threshold)      
        #singletons.sort(reverse = True) 
        with Pool(4) as p:
            yield p.map(self.mine_singleton,singletons)
        #return frequent
                
    def mine_singleton(self,singleton):
        threshold = self.minsup
        frequent = {}
        items = []
        auxDict = {}
        single = singleton        
        node = self.headers[single]            
        while node is not None:          
            frequent[tuple(node.name)] = frequent.get(tuple(node.name),0) + node.support
            items.clear()
            auxDict.clear()                
            if node.parent.parent is not None:                
                current = node
                suffix = (current.name,)
                auxDict[current.name] = current.support
                while current.parent.parent is not None:
                    current = current.parent
                    items.append(current.name)
                    auxDict[current.name] = current.support                
                for i in range(1,len(items)+1):
                    for subset in itertools.combinations(items,i):
                            pattern = tuple(sorted(list(subset +suffix)))
                            frequent[pattern] = frequent.get(pattern,0) + min([auxDict[x] for x in pattern])   
            node = node.link                
        #purge item with minsup smaller than Threshold
        for key in tuple(frequent.keys()):
            if frequent[key] < threshold:
                del frequent[key]
        return frequent

    def mine_itemsets(self, threshold,purge):
        """
        Mine the frequent itemsets using headers        
        """
        frequent = {}
        items = []
        auxDict = {}
        #Todo check if the singleton meet the minsup criteria. 
        if purge:
            singletons, purged = self.clean_singleton(threshold)       
        else:
            singletons = self.headers.keys()
            purged = []
        for single in singletons:
            frequent.clear()                  
            node = self.headers[single]            
            while node is not None:          
                frequent[tuple(node.name)] = frequent.get(tuple(node.name),0) + self.update_support(node,True) #node.support
                items.clear()
                auxDict.clear()                
                if node.parent.parent is not None:
                    current = node
                    suffix = (current.name,)
                    auxDict[current.name] = current.support
                    while current.parent.parent is not None:
                        current = current.parent
                        if not current.name in purged:
                            items.append(current.name)
                            auxDict[current.name] = self.update_support(current,True)             
                    for i in range(1,len(items)+1):
                        for subset in itertools.combinations(items,i):
                            pattern = tuple(sorted(list(subset +suffix)))
                            frequent[pattern] = frequent.get(pattern,0) + min([auxDict[x] for x in pattern])   
                node = node.link                
        #purge item with minsup smaller than Threshold
            for key in tuple(frequent.keys()):
                if frequent[key] < threshold:
                    del frequent[key]
            yield frequent
      
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

def printTransactions(dataset,threads):
    if type(dataset) is list:
        print(len(dataset))        
    elif type(dataset) is dict:
        print(len(dataset))        
    else:
        if threads:
            count =0        
            for i in dataset:                
                for j in i:
                    #print(type(j))
                    count += len(j)            
                #printTransactions(i)
            print(count)
        else:
            count =0        
            for i in dataset:
                count += len(i)            
                #printTransactions(i)
            print(count)



def main(argv):
    sys.setrecursionlimit(1000000)
    plaintext_database = ''
    preMinSup = 0
    minSup = 0
    sampleSize = 0
    threads = False
    batchSize = 1
    fading  = 0.6
    try:
        opts, args = getopt.getopt(argv, "hd:p:m:s:b:t:")
    except getopt.GetoptError:
        print("handler.py -d <database> -p <preMinSup> -m <minSup> -s <sampleSize> -b <batchSize> -t <True/False>")
        sys.exit(2)
    if len(opts) < 6:
        print("Please provide all parameters needed.")
        print("handler.py -d <database> -p <preMinSup> -m <minSup> -s <sampleSize> -b <batchSize> -t <True/False>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("handler.py -d <database> -p <preMinSup> -m <minSup> -s <sampleSize> -t <threads>")
            sys.exit()
        elif opt == '-d':
            plaintext_database = arg
        elif opt == '-p':
            preMinSup = float(arg)
        elif opt == '-m':
            minSup = float(arg)
        elif opt == '-s':
            sampleSize = int(arg)
        elif opt == '-b':
            batchSize = int(arg)
        elif opt == '-t':
            threads = bool(arg)
    
    test = loadData(plaintext_database,sampleSize)
    batches = [test[i:i + batchSize] for i in range(0, len(test), batchSize)]
    tree = Tree([], 1,fading,'None', 0)
    preMinSup *= batchSize
    minSup *= batchSize

    print("TimeFading Tree")
    print("PreMinSup- {}".format(preMinSup))
    print("Batch Size - {} ".format(batchSize))
    print("Sample Size - {}".format(sampleSize))
    start_time = time.time()
    for index in range(len(batches)):        
        tree.insert_transactions(batches[index],preMinSup)
        if ((index+1) % 10) == 0 :
            print("{}--- {:.4f} seconds ---".format(index+1, (time.time() - start_time)))
    #tree.root.display()

    start_time = time.time()
    print("Mining Sequential singleton Purge")
    print("Minsup - {}".format(minSup))
    printTransactions(tree.mine_itemsets(minSup, False),False)
    print("{}--- {} seconds ---".format("Mine with sequential code", (time.time() - start_time)))
    print()
    start_time = time.time()
    print("Mining Sequential Purge")
    print("Minsup - {}".format(minSup))
    printTransactions(tree.mine_itemsets(minSup, True),False)
    print("{}--- {} seconds ---".format("Mine with sequential code", (time.time() - start_time)))
    print("Mining with threads")
    print("Minsup - {}".format(minSup))
    print()
    start_time = time.time()
    printTransactions(tree.mine_itemsets_threadF(minSup),True)
    print("{}--- {} seconds ---".format("Mine with Parallel code", (time.time() - start_time)))
   

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(sys.argv[1:])
    else:
        main("-d ../T10I4D1000K.data -p 0.005 -m 0.02 -s 2000 -b 50 -t True".split())
    