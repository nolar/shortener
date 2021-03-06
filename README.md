## ABOUT

Yet another web shortener, which is going to save internet traffic costs and world energy consumption.


## FEATURES

* Multi-domain hosted. Each domain isolated from the others, but sharing the infrastructure.
* HTML and JSON APIs. HTML is very useful for quick human-friendly experiments, see below.
* Scalable. Everything is distributed & decentralized (some parts are not ready yet).
* Fast. Shortens in 10-20 ms, resolves and redirects in 1-2 ms (with MySQL).
* Stores its data in SimpleDB or MySQL (already works), or virtually any key-value capable storage.
* Implemented as standalone Python 2.7 library (2.6 is okay too).
* Django 1.3 is used for API entry points and response rendering.


## DEPLOYMENT

It is deployed at

* http://yaws.ws/

and all possible sub-domains:

* http://a.yaws.ws/
* http://9.yaws.ws/
* http://i.wanna.start.with.empty.yaws.ws/
* ...

Just go and try it. And remember it is in very prototype stage, so error handling
may be weak (it just dies with unified error template), and data may be lost.


## HISTORY

This was one of the test tasks for one of the positions in one of the companies
in one of the countries on one of the globes I've applied to.
But I'm so easily carried away with interesting tasks (not the first time)...

Nevertheless, since it has grown to a working deployable project, here is the code
shared. You are free to join, to fork, to have fun.


## CREDITS

Author: Sergey Vasilyev <nolar@nolar.info>, http://nolar.info/
