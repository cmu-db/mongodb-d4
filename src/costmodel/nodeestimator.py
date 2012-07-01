# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012 by Brown University
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------
import logging
import math

import catalog
from util.histogram import Histogram
import workload
from util import constants

LOG = logging.getLogger(__name__)

class NodeEstimator(object):

    def __init__(self, collections, num_nodes):
        assert type(collections) == dict
#        LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)
        self.collections = collections
        self.num_nodes = num_nodes

        # Keep track of how many times that we accessed each node
        self.nodeCounts = Histogram()
        self.op_count = 0
    ## DEF

    def reset(self):
        """
            Reset internal counters for this estimator.
            This should be called everytime we start evaluating a new design
        """
        self.nodeCounts.clear()
        self.op_count = 0
    ## DEF

    def estimateNodes(self, design, op):
        """
            For the given operation and a design object,
            return an estimate of a list of node ids that we think that
            the query will be executed on
        """

        results = [ ]
        broadcast = True
        shardingKeys = design.getShardKeys(op['collection'])

        if self.debug:
            LOG.debug("Computing node estimate for Op #%d [sharding=%s]", \
                      op['query_id'], shardingKeys)

        # Inserts always go to a single node
        if op['type'] == constants.OP_TYPE_INSERT:
            # Get the documents that they're trying to insert and then
            # compute their hashes based on the sharding key
            # Because there is no logical replication, each document will
            # be inserted in one and only one node
            for content in workload.getOpContents(op):
                values = catalog.getFieldValues(shardingKeys, content)
                results.append(self.computeTouchedNode(values))
            ## FOR
            broadcast = False

        # Network costs of SELECT, UPDATE, DELETE queries are based off
        # of using the sharding key in the predicate
        elif len(op['predicates']) > 0:
            predicate_types = set()
            for k,v in op['predicates'].iteritems() :
                if design.inShardKeyPattern(op['collection'], k) :
                    broadcast = False
                    predicate_types.add(v)
            if self.debug:
                LOG.debug("Op #%d %s Predicates: %s [broadcast=%s / predicateTypes=%s]",\
                          op['query_id'], op['collection'], op['predicates'], broadcast, list(predicate_types))

            ## ----------------------------------------------
            ## PRED_TYPE_REGEX
            ## ----------------------------------------------
            if not broadcast and constants.PRED_TYPE_REGEX in predicate_types:
                # Any query that is using a regex on the sharding key must be broadcast to every node
                # It's not complete accurate but it's just easier that way
                broadcast = True

            ## ----------------------------------------------
            ## PRED_TYPE_RANGE
            ## ----------------------------------------------
            elif not broadcast and constants.PRED_TYPE_RANGE in predicate_types:
                # If it's a scan, then we need to first figure out what
                # node they will start the scan at, and then just approximate
                # what it will do by adding N nodes to the touched list starting
                # from that first node. We will wrap around to zero
                num_touched = self.guessNodes(design, op['collection'], k)
                if self.debug:
                    LOG.info("Estimating that Op #%d on '%s' touches %d nodes",\
                             op["query_id"], op["collection"], num_touched)
                for content in workload.getOpContents(op):
                    values = catalog.getFieldValues(shardingKeys, content)
                    if self.debug: LOG.debug("%s -> %s", shardingKeys, values)
                    node_id = self.computeTouchedNode(values)
                    for i in xrange(num_touched):
                        if node_id >= self.num_nodes: node_id = 0
                        results.append(node_id)
                        node_id += 1
                    ## FOR
                ## FOR
            ## ----------------------------------------------
            ## PRED_TYPE_EQUALITY
            ## ----------------------------------------------
            elif not broadcast and constants.PRED_TYPE_EQUALITY in predicate_types:
                broadcast = False
                for content in workload.getOpContents(op):
                    values = catalog.getFieldValues(shardingKeys, content)
                    results.append(self.computeTouchedNode(values))
                ## FOR
            ## ----------------------------------------------
            ## BUSTED!
            ## ----------------------------------------------
            elif not broadcast:
                raise Exception("Unexpected predicate types '%s' for op #%d" % (list(predicate_types), op['query_id']))
        ## IF

        if broadcast:
            if self.debug: LOG.debug("Op #%d on '%s' is a broadcast query to all nodes",\
                                     op["query_id"], op["collection"])
            map(results.append, xrange(0, self.num_nodes))

        map(self.nodeCounts.put, results)
        self.op_count += 1
        return results
    ## DEF

    def computeTouchedNode(self, values):
        """
            Compute which node the given set of values will need to go
            This is just a simple (hash % N), where N is the number of nodes in the cluster
        """
        assert type(values) == tuple
        return hash(values) % self.num_nodes
    ## DEF

    def guessNodes(self, design, colName, fieldName):
        """
            Return the number of nodes that a query accessing a collection
            using the given field will touch.
            This serves as a stand-in for the EXPLAIN function referenced in the paper
        """
        col_info = self.collections[colName]
        if not fieldName in col_info['fields']:
            raise Exception("Invalid field '%s.%s" % (colName, fieldName))
        field = col_info['fields'][fieldName]

        # TODO: How do we use the statistics to determine the selectivity of this particular
        #       attribute and thus determine the number of nodes required to answer the query?
        return int(math.ceil(field['selectivity'] * self.num_nodes))
    ## DEF

    def getOpCount(self):
        """Return the number of operations evaluated"""
        return self.op_count
## CLASS