# Optimization

Optimization is performed on AWS lambda function every **last Thursday of the month at 12:30 PM**. The goal is to be able to change the different Optimization parameters directly on the Lambda interface, contrary to Fargate where it is impossible to change variables easily.

In this optimization step, we use Markowitz Portfolio Optimization to get the following elements:
- Every simulated portoflio during the Optimization
- Highest Sharpe Ratio Portfolio Allocation
- Lowest Variance Portfolio Allocation


The following parameters can be changed directly on the AWS interface:
-  **optim_env** = ['BNT/ETH','SNT/ETH','ENJ/ETH''LEND/ETH','LRC/ETH','POWR/ETH'] # The environment on which we optimize the portfolio
- **nportfolio** = 10000 # Number of portfolio simulations 
- **window** = 28 # Set the time window that will be used to compute expected return and asset correlations
- **nb_days** = 200 # Number of days kept in the dataframe for the study - nb_days have to be superior to window


