"""Set up default graphing parameters

This module contains functions that are used by multiple graphs in the our SPGC analysis
"""
import numpy as np

def get_graph_params():
    """Returns default parameters for graph generation
    """
    ###########
    ## Setup ##
    ###########
    # number of pt for column in latex-document
    fig_width_pt = 510  # single-column:510, double-column: 246; Get this from LaTeX using \showthe\columnwidth
    inches_per_pt = 1.1/72.27 # Convert pt to inches
    width_vs_height = (np.sqrt(5)-1.0)/2.0 # Ratio of height/width [(np.sqrt(5)-1.0)/2.0]
    fig_width = fig_width_pt*inches_per_pt  # width in inches
    fig_height = width_vs_height*fig_width  # height in inches
    fig_size = [fig_width,fig_height]

    # here you can set the parameters of the plot (fontsizes,...) in pt
    params = {'backend': 'ps',
              'axes.titlesize':16,
              'axes.labelsize': 14,
    #          'text.fontsize': 12,
              'legend.fontsize': 12,
    #           'figtext.fontsize': 12,
              'xtick.labelsize': 12,
              'ytick.labelsize': 12,

     #         'text.usetex': True,
     #         'ps.usedistiller' : 'xpdf',
              'figure.figsize': fig_size,
    #          'text.latex.unicode':True,
    #          'text.latex.preamble': [r'\usepackage{bm}'],

              'xtick.direction':'out',
              'ytick.direction':'out',

              'axes.spines.right' : False,
              'axes.spines.top' : False
             }
    return params
