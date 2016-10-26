#!/usr/bin/env python
# -*- coding: utf-8 -*-

import struct
import sys
import getopt
from collections import OrderedDict


# flags
LIR = 0
HIR = 1

# constants
MIN_STACK_FACTOR       = 1.0
MIN_CACHE_SIZE         = 200
DEFAULT_STACK_FACTOR   = 2
MIN_HIR_PCT            = 1
DEFAULT_HIR_PCT        = 1
MAX_HIR_PCT            = 100
MIN_HIR_RESIDENT       = 2

# program usage string
usageString = """usage: %s [options]
  -i <traceFile>         trace file path
  -s <cacheSize>         cache size in number of blocks
  -f <sizeLimitFactor>   size limit factor on S list
  -r <hirPercent>        HIR resident percentage of cache size
  -a                     read trace in ascii mode (default is parda)
  -h print this message (help)"""

class Entry:
    def __init__(self, key):
        self.key = key
        self.flag = HIR
        self.resident = True

    def __repr__(self):
        return "key=%s, flag=%s, resident=%s" % (self.key, self.flag, self.resident)

class LIRS:
    def __repr__(self):
        return "LIRS"

    def __init__(self, cacheSize, sizeLimitFactor, hirPercent):
        self.c = cacheSize
        self.lirs = 0
        self.hirs = 0
        self.maxhirs = max(MIN_HIR_RESIDENT, (hirPercent*0.01)*cacheSize)
        self.maxlirs = cacheSize - self.maxhirs
        self.S = OrderedDict()  # Big LRU queue (LIR blocks, non-resident HIR blocks and some resident HIR blocks)
        self.Q = OrderedDict()  # Small LRU queue (only resident HIR blocks)
        self.refs = 0
        self.misses = 0
        self.lastKey = None
        self.recordLirs = 0
        self.repeatedReference = False
        self.pruneCount = 0
        self.sizeLimitFactor = sizeLimitFactor
        self.maxSlength = sizeLimitFactor * cacheSize
        self.lastKey = None

    # processes rference through LIRS
    # returns True on a hit and False on a miss
    def processReference(self, key):
        self.refs += 1
        if key == self.lastKey:
            return True
        self.lastKey = key
        entry = None
        hit = False
        if key in self.S:
            entry = self.S[key]
            # LIR block, HIR non-resident or HIR resident block
            self.removeFromS(entry)
            if entry.flag == HIR:
                # A HIR block with lower reuse distance than the oldest LIR element,
                # this block now becomes LIR.
                if entry.resident:
                    self.removeFromQ(entry)
                    hit = True
                else:
                    # access to a non-resident HIR block is a miss
                    hit = False

                # Make space in Q for a migration
                if self.hirs >= self.maxhirs:
                    self.evictQentryLRU() # Make space for one HIR resident block in Q
                entry.flag = LIR
                entry.resident = True

                assert self.hirs <= self.maxhirs
                self.migrateLIRtoHIR()   # Change one LIR block to a HIR page
                self.prune() # the migrated block might have been LIR in the LRU position of S
            else:
                hit = True
                assert entry.flag == LIR and entry.resident
                self.prune()  #  If this was a LIR element at the bottom
        elif key in self.Q:
            # hit in Q
            hit = True
            entry = self.Q[key]
            assert key == entry.key
            assert entry.resident and entry.flag == HIR
            self.removeFromQ(entry)
            # reuse distance is large so we don't make it a LIR page
            # but promote to MRU in Q
            self.addQentryMRU(key, entry)
        else:
            # miss
            hit = False
            entry = Entry(key)
            # When a miss occurs and a free block is needed for replacement,
            # evict an HIR block that is resident in the cache

            if self.lirs < self.maxlirs:
                entry.flag = LIR   # Not using all the cache, make it a LIR page
            else:
                #remove the LRU from Q and insert this entry to the MRU position of Q
                # NOTE: The number of LIR blocks doesn't change.
                if self.hirs >= self.maxhirs:
                    self.evictQentryLRU()
                self.addQentryMRU(key, entry)
                assert entry.flag == HIR and entry.resident == True
        # always add the entry to the MRU of S
        self.addSentryMRU(key, entry)

        # sanity check
        assert self.peekSentryLRU().flag == LIR

        self.shrink()
        self.recordLirs = max(self.recordLirs, len(self.S))

        if not hit:
            self.misses += 1
        return hit

    # remove the LRU entry from S (should be LIR)
    # change it's flag to HIR and put it in the MRU position of Q
    def migrateLIRtoHIR(self):
        key, entry = self.popSentryLRU()
        #print "migrating %s from LIR to HIR" % key
        assert key == entry.key
        assert entry.flag == LIR and entry.resident
        entry.flag = HIR
        self.addQentryMRU(key, entry)
        # Switched this LIR block to HIR and put it to the MRU position of Q
        # Important! Don't put it back in S

    def prune(self):
        self.pruneCount += 1
        while self.S:
            entryLRU = self.peekSentryLRU()
            if entryLRU.flag == LIR:
                break
            key, entry = self.popSentryLRU()
            assert key == entry.key
            assert key == entryLRU.key
            assert entry.flag == HIR
            if entry.resident:
                assert key in self.Q
            else:
                assert key not in self.Q

    # evict the LRU entry from Q and mark it non-resident
    def evictQentryLRU(self):
        assert self.hirs >= self.maxhirs
        key, entry = self.popQentryLRU()
        assert entry.flag == HIR and entry.resident
        entry.resident = False  # It is maybe in S
        assert self.hirs <= self.maxhirs

    # ensure that the size of S is at most self.maxSlength
    def shrink(self):
        # TODO: Optimize this by caching the pointer.
        if len(self.S) > self.maxSlength:
            # Remove oldest HIR block from S
            # This iterates from LRU to MRU
            for key, entry in self.S.iteritems():
                if entry.flag == HIR:
                    del self.S[key]
                    break
        assert len(self.S) <= self.maxSlength

    # remove entry from S, regardless of location
    def removeFromS(self, entry):
        del self.S[entry.key]
        if entry.flag == LIR and entry.resident:
            self.lirs -= 1

    # remove entry from Q, regardless of location
    def removeFromQ(self, entry):
        del self.Q[entry.key]
        self.hirs -= 1
        assert entry.resident

    # insert key -> entry to the MRU position of S
    def addSentryMRU(self, key, entry):
        self.S[key] = entry
        if entry.flag == LIR and entry.resident:
            self.lirs += 1

    # insert key -> entry to the MRU position of Q
    def addQentryMRU(self, key, entry):
        self.Q[key] = entry
        self.hirs += 1

    # pops the LRU entry from S
    def popSentryLRU(self):
        key, entry = self.S.popitem(last=False)
        if entry.flag == LIR and entry.resident:
            self.lirs -= 1
        return key, entry

    # pops the LRU entry from Q
    def popQentryLRU(self):
        key, entry = self.Q.popitem(last=False)
        self.hirs -= 1
        return key, entry

    def peekSentryLRU(self):
        return self.S.itervalues().next()

    def peekQentryLRU(self):
        return self.Q.itervalues().next()

    def print_statistics(self):
        print "Memory size                        = %d" % self.c
        print "Max S size                         = %d" % self.maxSlength
        print "Llirs (max reached size of S)      = %d" % self.recordLirs
        print "Lhirs (cache size for HIR blocks)  = %d" % self.hirs
        print "Final blocks refs                  = %d" % self.refs
        print "Final number of misses             = %d" % self.misses
        print "Final hit rate                     = %2.5lf%%" % (100.0 * (1.0 - self.misses/float(self.refs)))
        print "Prune count                        = %d" % self.pruneCount


def Usage(appName):
    print usageString % appName
    exit()

def main():
    traceFileName = None
    cacheSize = 0
    appName = sys.argv[0]
    sizeLimitFactor = DEFAULT_STACK_FACTOR
    asciiMode = False
    hirPercent = DEFAULT_HIR_PCT
    options, remainder = getopt.getopt(sys.argv[1:],
                                       'i:s:f:r:ah',
                                       ['-i', '-s', '-f', '-a', '-r', '-h'])

    for opt, arg in options:
        if opt == "-i":
            traceFileName = arg
        elif opt == "-s":
            cacheSize = int(arg)
        elif opt == "-f":
            sizeLimitFactor = float(arg)
        elif opt == "-r":
            hirPercent = int(arg)
        elif opt == "-a":
            asciiMode = True
        elif opt == "-h":
            Usage(appName)
            return

    if traceFileName == None:
        print "Please provide a trace file"
        Usage(appName)

    if sizeLimitFactor < MIN_STACK_FACTOR:
        print "Please provide a stack factor > %f" % MIN_STACK_FACTOR
        Usage(appName)

    if cacheSize < MIN_CACHE_SIZE:
        print "Please provide a cache size > %d" % MIN_CACHE_SIZE
        Usage(appName)

    if hirPercent < MIN_HIR_PCT or hirPercent > MAX_HIR_PCT:
        print "Please provide a HIR percent value in [%d,%d]" % (MIN_HIR_PCT, MAX_HIR_PCT)
        Usage(appName)

    alg = LIRS(cacheSize, sizeLimitFactor, hirPercent)

    if asciiMode:
        with open(traceFileName, "r") as f:
            last = None
            for line in f.xreadlines():
                line = line.strip()
                if line == "*":
                    continue

                block = int(line)
                alg.processReference(block)
                last = line
    else:
        with open(traceFileName, "rb") as f:
            lastBlock = None
            while True:
                data = f.read(8)
                if not data:
                    # end of file
                    break

                block = struct.unpack('Q', data)
                alg.processReference(block)
                lastBlock = block

    alg.print_statistics()

if __name__ == "__main__":
    main()
