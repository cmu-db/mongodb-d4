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
from util import constants

LOG = logging.getLogger(__name__)

class NodeEstimator(object):

    def __init__(self, num_nodes):
#        LOG.setLevel(logging.DEBUG)
        self.debug = LOG.isEnabledFor(logging.DEBUG)
        self.num_nodes = num_nodes
    ## DEF

    def estimateOp(self, design, op):
        """
            For the given operation and a design object,
            return an estimate of a list of node ids that we think that
            the query will be executed on
        """

        results = [ ]

        # Inserts always go to a single node
        if op['type'] == constants.OP_TYPE_INSERT:
            # result += 1
            results.append(0) # FIXME
        # Network costs of SELECT, UPDATE, DELETE queries are based off
        # of using the sharding key in the predicate
        else:
            if len(op['predicates']) > 0:
                scan = True
                predicate_type = None
                for k,v in op['predicates'].iteritems() :
                    if design.inShardKeyPattern(op['collection'], k) :
                        scan = False
                        predicate_type = v
                if self.debug:
                    LOG.debug("Op #%d %s Predicates: %s [scan=%s / predicateType=%s]",\
                              op['query_id'], op['collection'], op['predicates'], scan, predicate_type)
                if not scan:
                    # Query uses shard key... need to determine if this is an
                    # equality predicate or a range type
                    if predicate_type == constants.PRED_TYPE_EQUALITY:
                        # results += 0.0
                        results.append(0) # FIXME
                        pass
                    else:
                        nodes = self.guessNodes(design, op['collection'], k)
                        LOG.info("Estimating that Op #%d on '%s' touches %d nodes",\
                            op["query_id"], op["collection"], nodes)
                        for i in xrange(0, nodes):
                            results.append(i)
                else:
                    if self.debug:
                        LOG.debug("Op #%d on '%s' is a broadcast query",\
                            op["query_id"], op["collection"])
#                    result += self.nodes
                    map(results.append, xrange(0, self.num_nodes))
            else:
                map(results.append, xrange(0, self.num_nodes))
#                result += self.nodes

        return results
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
        return math.ceil(field['selectivity'] * self.num_nodes)
    ## DEF
## CLASS