import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)
from model import Model
import utils
from agent import Agent
from trainner import MultiTrainer
from alldatasets.codecontestplus import CodeContestPlus
def one_step(model,question):
    candidates=model.generate_candidates(3,3,question)
def main():
