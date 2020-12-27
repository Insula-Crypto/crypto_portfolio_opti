# Optimization

Optimization is performed on AWS lambda function every **last Thursday of the month at 12:00 PM GMT**. The goal is to be able to change the different Optimization parameters directly on the Lambda interface, contrary to Fargate where it is impossible to change variables easily.

In this optimization step, we use Markowitz Portfolio Optimization to get the following elements:
- Every simulated portoflio during the Optimization
- Highest Sharpe Ratio Portfolio Allocation
- Lowest Variance Portfolio Allocation


The following parameters can be changed directly on the AWS interface:
-  **optim_env** = ['BNT/ETH','SNT/ETH','ENJ/ETH''LEND/ETH','LRC/ETH','POWR/ETH'] # The environment on which we optimize the portfolio
- **nportfolio** = 10000 # Number of portfolio simulations 
- **window** = 60 # Number of days we consider in the Optimization

