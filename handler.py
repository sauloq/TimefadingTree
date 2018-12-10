import sys
import itertools
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
        print(' '*ind,self.name,' ', self.support)
        for child in self.children:
            child.display(ind+1)   


class Tree (object):
    """
    A stream tree.
    """
    def __init__(self, transactions,threshold,root_value,root_count):
        """
        Initialization of the tree
        """
        self.frequent = self.find_frequent(transactions,threshold)
        self.headers = self.build_header_table(self.frequent)
        self.root = self.build_tree(transactions,root_value,root_count,self.frequent,self.headers)

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
                       

    def insert_transactions(self, transactions, threshold):
        """
        Function to insert new batches.
        """
        self.frequent = self.find_frequent(transactions,threshold)
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

    def tree_has_single_path (self, node):
        """
        Verify if the sub-tree of this node is a single path tree.
        """
        num_children = len(node.children)
        if num_children > 1:
            return False
        elif num_children == 0:
            return True
        else:
            return self.tree_has_single_path(node.children[0]) # It may raise a exception if the tree is huge. (check ways to improve the size of the recursive heap)
    def mine_itemsets (self, threshold):
        """
        Mine the frequent itemsets
        """
        if self.tree_has_single_path(self.root):
            return self.generate_pattern_list()
        else:
            return self.zip_patterns(self.mine_sub_trees(threshold))

    def zip_patterns(self, patterns):
        """
        Append suffix to patterns in dictionary if we are in a conditional tree.
        """
        suffix = self.root.name
        if suffix is not None:
            new_patterns = {}
            for key in patterns.keys():
                new_patterns[tuple(sorted(list(key)+[suffix]))] = patterns[key]
            return new_patterns
        return patterns
    def generate_pattern_list(self):
        """
        Generate a list of patterns with support counts
        """
        patterns = {}
        items = self.frequent.keys()
        #if we are in a conditional tree the suffix is a patterns itself.
        if self.root.name is None:
            suffix_value = []
        else:
            suffix_value = [self.root.name]
            patterns[tuple(suffix_value)] = self.root.support
        for i in range(1, len(items)+1):
            for subset in itertools.combinations(items,i):
                pattern = tuple(sorted(list(subset) + suffix_value))
                patterns[pattern] = min([self.frequent[x] for x in subset])
        return patterns
    def mine_sub_trees(self, threshold):
        """
        Generate subtrees and mine them for patterns
        """
        patterns = {}
        mining_order = sorted(self.frequent.keys(), key = lambda x: self.frequent[x])
        # get items in tree in reverse order of occurences.
        for item in mining_order:
            suffixes = []
            conditional_tree_input = []
            node = self.headers[item]
            #Follow node links to get the list of all occurences of a certain item.
            while node is not None:
                suffixes.append(node)
                node = node.link
            # for each occurence of the item, trace the path back to the root node.
            for suffix in suffixes:
                frequency = suffix.support
                path = []
                parent = suffix.parent

                while parent.parent is not None:
                    path.append(parent.name)
                    parent = parent.parent

                for i in range(frequency):
                    conditional_tree_input.append(path)
            #Construc the subtree in order to get the patterns
            subtree = Tree(conditional_tree_input,threshold,item, self.frequent[item])
            subtree_patterns = subtree.mine_itemsets(threshold)
            for pattern in subtree_patterns.keys():
                if pattern in patterns:
                    patterns[pattern] += subtree_patterns[pattern]
                else:
                    patterns[pattern] = subtree_patterns[pattern]
        return patterns


def find_frequent_itemsets(transactions, support_treshould):
        """
        Given the transactions mine the Frequent itemsets.
        """
        tree = Tree(transactions, support_treshould,None, None)
        return tree.mine_itemsets(support_treshould)


def loadData (data):
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
    return result



def printTransactions(dataset):
    for i in dataset:
        print(i)


test = loadData('/Users/dossants/Desktop/DataMining/Project/IBMGenerator-master/T10I4D100K.data')
printTransactions(test[0:20])
tree = Tree(test[0:20], 1,None, None)
print(tree.mine_itemsets(2))
printTransactions(test[21:40])
tree.insert_transactions(test[21:40],1)
print(tree.mine_itemsets(2))

