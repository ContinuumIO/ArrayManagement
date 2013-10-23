from arraymanagement.nodes import csvnodes
pattern_priority = ['*pipe.csv']
loaders = {
    '*.CSV' : csvnodes.PandasCSVTable,
    '*pipe.csv' : csvnodes.PandasCSVTable,
    }
