//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// LSTM_Model.py
//
// Identification: src/brain/modelgen/LSTM_Model.py
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#===----------------------------------------------------------------------===#
#
#                         Peloton
#
# LSTM_Model.py
#
# Identification: src/brain/modelgen/LSTM_Model.py
#
# Copyright (c) 2015-2018, Carnegie Mellon University Database Group
#
#===----------------------------------------------------------------------===#

import tensorflow as tf
import functools
import os
import argparse

def lazy_property(function):
    attribute = '_cache_' + function.__name__

    @property
    @functools.wraps(function)
    def decorator(self):
        if not hasattr(self, attribute):
            setattr(self, attribute, function(self))
        return getattr(self, attribute)

    return decorator

class LSTM_Model:

    def __init__(self, ntoken, ninp, nhid, nlayers, lr=0.001,
                 dropout_ratio=0.5, clip_norm = 0.5, **kwargs):
        """
        :param ntoken: #features(input to encoder)
        :param ninp: input_size to LSTM(output of encoder)
        :param nhid: hidden layers in LSTM
        :param nlayers: number of layers
        :param dropout: dropout rate
        """
        tf.reset_default_graph()
        self.data = tf.placeholder(tf.float32, [None, None, ntoken], name="data_")
        self.target =  tf.placeholder(tf.float32, [None, None, ntoken], name="target_")
        self._ntoken = ntoken
        self._ninp = ninp
        self._nhid = nhid
        self._nlayers = nlayers
        # Setting to defaults known to work well
        self._lr = tf.placeholder_with_default(lr, shape=None,
                                               name="learn_rate_")
        self._dropout_ratio = tf.placeholder_with_default(dropout_ratio, shape=None,
                                                          name="dropout_ratio_")
        self._clip_norm = tf.placeholder_with_default(clip_norm, shape=None,
                                                      name="clip_norm_")
        self.tf_init = tf.global_variables_initializer
        self.prediction
        self.loss
        self.optimize


    @staticmethod
    def stacked_lstm_cell(num_cells, hid_units, dropout):
        cells = []
        for i in range(num_cells):
            cells.append(tf.nn.rnn_cell.DropoutWrapper(tf.nn.rnn_cell.LSTMCell(hid_units),
                                                       output_keep_prob=dropout,
                                                       variational_recurrent=False,
                                                       dtype=tf.float32))
        return tf.nn.rnn_cell.MultiRNNCell(cells)

    @lazy_property
    def prediction(self):
        net = self.data
        kernel_init = tf.random_normal_initializer()
        with tf.name_scope("input_linear_layer"):
            net_shape = tf.shape(net)
            bsz = net_shape[0]
            bptt = net_shape[1]
            net = tf.reshape(self.data, [-1, self._ntoken])
            net = tf.layers.dense(net, self._ninp,
                                  activation=tf.nn.leaky_relu,
                                  kernel_initializer=kernel_init)
            net = tf.reshape(net, [bsz, bptt, self._ninp])
        with tf.name_scope("stacked_lstm_cell"):
            stacked_lstm_cell = self.stacked_lstm_cell(self._nlayers,
                                                       self._nhid,
                                                       self._dropout_ratio)
            # If GPU is present, should use highly optimized CudnnLSTMCell
            net, _ = tf.nn.dynamic_rnn(stacked_lstm_cell,
                                          net, dtype=tf.float32,
                                          time_major=False)
        with tf.name_scope("output_linear_layer"):
            net = tf.reshape(net, [-1, self._nhid])
            net = tf.layers.dense(net, self._ntoken,
                                  activation=tf.nn.leaky_relu,
                                  kernel_initializer=kernel_init)
        net = tf.reshape(net, [bsz, bptt, -1], name="pred_")
        return net

    @lazy_property
    def loss(self):
        loss = tf.reduce_mean(tf.squared_difference(self.target, self.prediction), name='lossOp_')
        return loss

    @lazy_property
    def optimize(self):
        params = tf.trainable_variables()
        gradients = tf.gradients(self.loss, params)
        clipped_gradients, _ = tf.clip_by_global_norm(
            gradients, self._clip_norm)
        optimizer = tf.train.AdamOptimizer(learning_rate=self._lr)
        return optimizer.apply_gradients(zip(clipped_gradients,
                                             params), name="optimizeOp_")

    def write_graph(self, dir):
        fname = "{}.pb".format(self.__repr__())
        abs_path = os.path.join(dir, fname)
        if not os.path.exists(abs_path):
            tf.train.write_graph(tf.get_default_graph(),
                                 dir, fname, False)


    def __repr__(self):
        return "LSTM"