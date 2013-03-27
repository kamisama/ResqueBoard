#ResqueBoard [![Build Status](https://travis-ci.org/kamisama/ResqueBoard.png?branch=dev)](https://travis-ci.org/kamisama/ResqueBoard)

ResqueBoard is a web interface to monitor your php-resque activities.

It's different from the other web interface, like the one shipped with the [original resque](https://github.com/defunkt/resque/#the-front-end), that display only what's happening right now, and then forget it.  
ResqueBoard can log *absolutely everything* your workers do, then compute various metrics, in addition to monitoring your workers and jobs health in realtime.

##Goals
ResqueBoard is built for 2 objectives :

* see what's happening right now in realtime
* visualize what's happened in the past with various charts, to easily benchmarks and balance your workers

Learn more on the [official website](http://resqueboard.kamisama.me), or take a look at the [demo](http://resque.kamisama.me/).
