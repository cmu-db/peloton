#===----------------------------------------------------------------------===#
#
#                         Peloton
#
# AugmentedNN.py
#
# Identification: src/brain/modelgen/AugmentedNN.py
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

class AugmentedNN:

    def __init__(self, column_num, order=1, neuron_num=16, lr=0.1, **kwargs):
        tf.reset_default_graph()
        self.data = tf.placeholder(tf.float32, [None, column_num*2], name="data_")
        self.target =  tf.placeholder(tf.float32, [None, 1], name="target_")
        self._column_num = column_num
        self._order = order
        self._neuron_num = neuron_num
        self._lr = tf.placeholder_with_default(lr, shape=None,
                                               name="learn_rate_")
        self.tf_init = tf.global_variables_initializer
        self.prediction
        self.loss
        self.optimize

    @staticmethod
    def jump_activation(k):
        """
        This is an activation function used to learn discontinuous functions.
        Reference: https://dl.acm.org/citation.cfm?id=2326898
        """
        def jump_activation_k(x):
            return tf.pow(tf.maximum(0.0, 1-tf.exp(-x)), k)
        return jump_activation_k

    @lazy_property
    def prediction(self):
        net = self.data
        kernel_init = tf.random_normal_initializer(mean=0.0001, stddev=0.0001)
        with tf.name_scope("hidden_layer"):
            net_shape = tf.shape(net)
            bsz = net_shape[0]

            h1_layers = []
            for i in range(1, self._order+1):
                h1 = tf.layers.dense(net, self._neuron_num, 
                                     activation=self.jump_activation(i),
                                     kernel_initializer=kernel_init)
                h1_layers.append(h1)
            h1_layers = tf.concat(h1_layers, 1)        
        with tf.name_scope("output_layer"):
            net = tf.layers.dense(h1_layers, 1, 
                                  activation=self.jump_activation(1),
                                  kernel_initializer=kernel_init)
        net = tf.reshape(net, [bsz, -1], name="pred_")
        return net

    @lazy_property
    def loss(self):
        loss = tf.reduce_mean(tf.squared_difference(self.target, self.prediction), name='lossOp_')
        return loss

    @lazy_property
    def optimize(self):
        params = tf.trainable_variables()
        gradients = tf.gradients(self.loss, params)
        optimizer = tf.train.AdagradOptimizer(learning_rate=self._lr)
        return optimizer.apply_gradients(zip(gradients,
                                             params), name="optimizeOp_")

    def write_graph(self, dir):
        fname = "{}.pb".format(self.__repr__())
        abs_path = os.path.join(dir, fname)
        if not os.path.exists(abs_path):
            tf.train.write_graph(tf.get_default_graph(),
                                 dir, fname, False)

    def __repr__(self):
        return "augmented_nn"

def main():
    parser = argparse.ArgumentParser(description='AugmentedNN Model Generator')

    parser.add_argument('--column_num', type=int, default=1, help='Number of augmentedNN Hidden units')
    parser.add_argument('--order', type=int, default=3, help='Max order of activation function')
    parser.add_argument('--neuron_num', type=int, default=20, help='Number of neurons in hidden layer')
    parser.add_argument('--lr', type=float, default=0.001, help='Learning rate')
    parser.add_argument('graph_out_path', type=str, help='Path to write graph output', nargs='+')
    args = parser.parse_args()
    model = AugmentedNN(args.column_num, args.order, args.neuron_num, args.lr)
    model.tf_init()
    model.write_graph(' '.join(args.graph_out_path))

if __name__ == '__main__':
    main()
