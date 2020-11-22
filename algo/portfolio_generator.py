"""
- 15 meilleur sharp
- ajouter un des meilleur sharp décorélé
- envoyer à Andre
"""
import heapq

from algo.asset import Asset
from algo.data import START_DATE, END_DATE
from algo.database import Database
from algo.formula import compute_sharp_ratio

NB_BASE_ASSET = 15
CORRELATION_THRESHOLD = -0.8


class PortfolioGenerator:

    def load_sharpe_heap(self, assets: [Asset], db: Database):
        for asset in assets:
            self.sharpes.append(compute_sharp_ratio(asset, START_DATE, END_DATE, db), asset)
        heapq._heapify_max(self.sharpes)

    def __init__(self):
        self.portfolio = []
        self.sharpes = []
        self.load_sharpe_heap()
        for i in range(NB_BASE_ASSET):
            self.portfolio.append(heapq._heappop_max(self.sharpes))

    def generate_portfolio(self, db: Database):
        while len(self.sharpes):
            (sharpe, asset) = heapq._heappop_max(self.sharpes)
            correlation = []
            for i in range(NB_BASE_ASSET):
                correlation.append(compute_sharp_ratio(self.portfolio[i][1], asset, START_DATE, END_DATE, db))
            if sum(correlation) / len(correlation) <= 0.8:
                self.portfolio.append((sharpe, asset))
                yield self.portfolio
                self.portfolio.pop()
