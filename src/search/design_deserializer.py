from design import Design
import sys

class Deserializer:
    def __init__(self, file_path):
        f = open(file_path, 'r')
        self.lines = f.readlines()
        f.close()
    ## DEF

    def Deserialize(self):
        d = Design()
        self.__deserialize__(self.lines, d)
        return d
    ## DEF
    
    def __deserialize__(self, lines, design):
        """
            We treat every four lines as a unit, which means a design for one collection
        """
        # Remove all empty lines
        valid_lines = []
        for line in lines:
            if len(line) != 0:
                valid_lines.append(line)
            ## IF
        ## FOR
        assert len(valid_lines) % 4 == 0
        num_collection = len(valid_lines) / 4

        for i in xrange(num_collection):
            collection_design_lines = lines[i*4:i*4+4]
            self.__add_to_design__(collection_design_lines, design)
        ## FOR
    ## DEF

    def __add_to_design__(self, lines, design):
        # Get collection name
        col_name = lines[0].strip().split(' ')[1]
        design.addCollection(col_name)
        # Get denrom
        denorm = lines[1][lines[1].index('denorm:') + len('denorm') + 1:].strip()
        if denorm != "None":
            design.setDenormalizationParent(col_name, denorm)
        # Get shardKeys
        t = lines[2][lines[2].index('shardKeys') + len('shardKeys') + 1:].strip()[1:-1]
        shardKeys = []
        if len(t) > 0:
            for s in t.split(','):
                if len(s) > 0:
                    start_index = 2 if s.strip()[0] == 'u' else 1
                    shardKeys.append(unicode(s.strip()[1:-1]))
            ## FOR
            design.addShardKey(col_name, shardKeys)
        ## IF
        # Get indexes
        t = lines[3][lines[3].index('indexes') + len('indexes') + 1:].strip()[1:-1]
        if len(t) > 0:
            for s in self.__get_indexes_splits__(t):
                index = []
                for split in s.split(','):
                    if len(split) > 0:
                        start_index = 2 if split.strip()[0] == 'u' else 1
                        index.append(unicode(split.strip()[start_index:-1]))
                ## FOR
                design.addIndex(col_name, index)
            ## FOR
        ## IF
    ## DEF

    def __get_indexes_splits__(self, line):
        indexes = []
        while len(line) > 0:
            begin_index = line.index('(')
            end_index = line.index(')') + 1
            s = line[begin_index:end_index]
            indexes.append(s[1:-1])
            line = line[end_index:]
        ## WHILE
        return indexes
    ## DEF
## CLASS

if __name__ == "__main__":
    if len(sys.argv) < 2:
        exit()
    ds = Deserializer(sys.argv[1])
    print ds.Deserialize()