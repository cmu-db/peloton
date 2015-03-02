# Very hacky proof of concept for bitset join (late materialization).

# TODO Don't allow duplicate base tables.
# TODO Don't access internals of tile from join operator

def mult(L):
    return reduce(lambda p,n: p * n, L, 1)

# Maps to the row in that specific tile corresponding to
# the tile index.
def validBitsBijection(row, tileIndex, tiles):
    baseTileRow = row
    for i in xrange(1, tileIndex + 1):
        # Inefficient! Precompute suffix multiples.
        baseTileRow %= mult([t.size() for t in tiles[i:]])

    baseTileRow /= mult([t.size() for t in tiles[tileIndex+1:]])

    """ Debugging purposes.
    print [t.size() for t in tiles]
    print "row: %d, baseTileRow: %d, tileIndex: %d" % (row, baseTileRow, tileIndex)
    """
    return baseTileRow

class Tile:
    def __init__(self, isPhysicalTile):

        # Not familiar with subclassing in python so using a flag here instead.
        self.isPhysicalTile = isPhysicalTile

        # Only set if physical tile.
        self.data = []

        # Only set if logical tile.
        self.baseTiles = []

        self.validBits = []
        self.schema = []

    def setData(self, data):
        assert self.isPhysicalTile
        self.data = data
        self.setValidBits([True] * len(data))
        for i in xrange(len(data[0])):
            self.schema.append((-1, i))
    
    def addBaseTile(self, baseTile):
        assert not self.isPhysicalTile
        self.baseTiles.append(baseTile)

    def setValidBits(self, validBits):
        # Ensure that number of valid bits is the same as the total
        # number of rows.
        assert (self.isPhysicalTile and len(validBits) == len(self.data)) \
               or (mult([t.size() for t in self.baseTiles]))
        self.validBits = validBits

    def setSchema(self, schema):
        self.schema = schema

    def getSchema(self):
        return self.schema[:]

    def getBaseTile(self, index):
        return self.baseTiles[index]

    def getBaseTiles(self):
        return self.baseTiles[:]

    def size(self):
        return len(self.validBits)

    def numCols(self):
        return len(self.schema)

    def getField(self, row, col):
        assert row < len(self.validBits)
        assert col < len(self.schema)

        if not self.validBits[row]:
            return None

        if self.isPhysicalTile:
            return self.data[row][col]

        # Check schema to determine which base tile the column belongs to.
        (baseTileIndex, baseTileColumn) = self.schema[col]
        baseTile = self.baseTiles[baseTileIndex]

        # Compute which row in the base tile this row refers to.
        baseTileRow = validBitsBijection(row, baseTileIndex, self.baseTiles)

        return self.baseTiles[baseTileIndex].getField(
            baseTileRow, baseTileColumn)
            
def printTile(tile):
    for i in xrange(tile.size()):
        for j in xrange(tile.numCols()):
            print tile.getField(i, j), ',',
        print '\n'

# Helper for join...
# Very hacky.
def addSchema(newTile, oldTile):
    currentIndex = len(newTile.getBaseTiles()) - 1
    newTileSchema = newTile.getSchema()
    prevBaseTileIndex = -1
    for (baseTileIndex, baseTileCol) in oldTile.getSchema():
        if not oldTile.isPhysicalTile:
            # Sanity check. We use -1 for physical tile schemas.
            assert baseTileIndex >= 0
            # We assume schema tuples of the same base tiles are contiguous.
            # Hacky assumption.
            if prevBaseTileIndex != baseTileIndex:
                currentIndex += 1
                prevBaseTileIndex = baseTileIndex
                newTile.addBaseTile(oldTile.getBaseTile(baseTileIndex))
            newTileSchema.append((currentIndex, baseTileCol))
        else:
            assert baseTileIndex == -1
            newTileSchema.append((currentIndex+1, baseTileCol))

    if oldTile.isPhysicalTile:
        newTile.addBaseTile(oldTile)

    newTile.setSchema(newTileSchema)

# Right now it's just a cartesian product...
def join(tile1, tile2):
    newTile = Tile(False)
    addSchema(newTile, tile1)
    addSchema(newTile, tile2)

    size1 = tile1.size()
    size2 = tile2.size()

    newValidBits = []
    for i in xrange(size1 * size2):
        valid = True
        for j in xrange(len(newTile.baseTiles)):
            row = validBitsBijection(i, j, newTile.baseTiles)
            # Naive implementation. Can save by looking through tile1/tile2
            # instead of their base tables?
            if newTile.baseTiles[j].getField(row, 0) == None:
                valid = False
                break
        newValidBits.append(valid)

    newTile.setValidBits(newValidBits)
    return newTile

baseTile1 = Tile(True)
data1 = [
['john', 'doe', 1],
['harry', 'potter', 2],
['justin', 'bieber', 3]]
baseTile1.setData(data1)

baseTile2 = Tile(True)
data2 = [
[123, 456],
[222, 333]]
baseTile2.setData(data2)

baseTile3 = Tile(True)
data3 = [[7], [8], [9]]
baseTile3.setData(data3)

logicalTile1 = join(baseTile2, baseTile3)

logicalTile2 = join(baseTile1, logicalTile1)

logicalTile3 = join(baseTile1, baseTile2)
logicalTile4 = join(logicalTile3, baseTile3)
printTile(logicalTile4)
