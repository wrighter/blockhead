# blockhead

## Crypto trading strategies and execution.

Once blockhead is checked out and [Anaconda 3.6](https://www.anaconda.com/download/) is installed, do an install of the requirements

```
conda create -n blockhead
source activate blockhead
```

You can now downlaod gdax data using the [gdax-python](https://github.com/danpaquin/gdax-python) api. Install it from HEAD in your environment

```
python setup.py install
```

We also need some modules not in anaconda
```
pip install tzlocal
```

