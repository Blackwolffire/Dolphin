# Dolphin - EPITA group 12

###  Our approach
To not be limited by the JUMP API, we decided to replicate the database and store every asset and quote locally.
We then created a correlation matrix, by calculating the correlation between each asset over the designated
period, and stored it in database for fast access and advanced querying capabilites.

### Computing architecture
To compute our sharpe ratios faster, we opted for a decentralized way of computing, using a consumer-producer design pattern.
This allowed to run multiple consumers on several servers, each computing sharpes for portfolios given by the producer. 
We were able to compute the sharpe ratio of 2pf/second, much faster than the JUMP Api which allowed us 0.1pf/second.

### Mathematics and algorithms
The Sharpe Ratio = (Excess Return)/Volatility.

We computed the Excess Return as the return between the start and the end of the period, annualized, minus 0.05%.
We computed the volatility as the standard deviation of the daily returns, annualized.

### Generation of Portfolio
We started by choosing the 15 best assets according to their sharpe ratio and tried to add other good asset or decorrelated
assets. Then tuned the weight of each asset in a portfolio according to the sharpe ratio again.