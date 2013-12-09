from arraymanagement.nodes import csvnodes
loaders = {
    '*.CSV' : csvnodes.PandasCSVTable,
    '*pipe.csv' : csvnodes.PandasCSVTable,
    }

config = {'loaders' : loaders}
