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
    """Container module with an encoder, a recurrent module, and a decoder.
    Encoder-decoder LSTM is useful for Seq2Seq prediction problems
    i.e. where we have a sequence as input(previous workload info) and want to predict a sequence
    as output(workload prediction 10 days later for 1 entire day)
    Learning rate used: https:#www.tensorflow.org/tutorials/seq2seq - Check Gradient computation & optimization
    Its provided: The value of learning_rate can is usually in the range 0.0001 to 0.001;
    and can be set to decrease as training progresses
    """

    def __init__(self, ntoken, ninp, nhid, nlayers, lr=0.001,
                 dropout_ratio=0.5, clip_norm = 0.5, **kwargs):
        """
        :param ntoken: #features(input to encoder)
        :param ninp: input_size to LSTM(output of encoder)
        :param nhid: hidden layers in LSTM
        :param nlayers:
        :param dropout:
        :param tie_weights:
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


    @lazy_property
    def prediction(self):
        # Recurrent network.
        # Define weights
        weights = {
            'encoder': tf.Variable(tf.random_normal([self._ntoken, self._ninp]), name="enc_wt"),
            'decoder': tf.Variable(tf.random_normal([self._nhid, self._ntoken]), name="dec_wt")
        }
        biases = {
            'encoder': tf.Variable(tf.random_normal([self._ninp]), name="enc_bias"),
            'decoder': tf.Variable(tf.random_normal([self._ntoken]), name="dec_bias")
        }
        # Reshape inputs to feed to encoder
        bptt = tf.shape(self.data)[0]
        bsz = tf.shape(self.data)[1]
        input = tf.reshape(self.data, [bptt * bsz, self._ntoken])
        # Apply encoder to get workload-embeddings
        emb = tf.matmul(input, weights["encoder"]) + biases["encoder"]
        # Reshape embeddings to feed to Stacked LSTM
        emb = tf.reshape(emb, [bptt, bsz, self._ninp])
        stacked_lstm_cell = tf.nn.rnn_cell.\
            MultiRNNCell([tf.nn.rnn_cell.DropoutWrapper(tf.nn.rnn_cell.LSTMCell(self._nhid),
                                                        output_keep_prob=self._dropout_ratio)
                          for _ in range(self._nlayers)])
        # time_major true => time steps x batch size  x features.
        # time_major false => batch_size x time_steps x features
        output, _ = tf.nn.dynamic_rnn(stacked_lstm_cell,
                                      emb, dtype=tf.float32,
                                      time_major=False)
        # Apply decoder to get output
        decoder_in = tf.reshape(output, [bptt * bsz, self._nhid])
        decoded = tf.matmul(decoder_in, weights["decoder"]) + biases["decoder"]
        pred = tf.reshape(decoded, [bptt, bsz, -1], name="pred_")
        return pred

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