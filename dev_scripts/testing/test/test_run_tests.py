#import logging
#import os
#from typing import List, Tuple
#
#import pytest
#
#import dev_scripts.linter2 as lntr
#import dev_scripts.url as url
#import helpers.conda as hco
#import helpers.dbg as dbg
#import helpers.env as env
#import helpers.git as git
#import helpers.io_ as io_
#import helpers.system_interaction as si
#import helpers.unit_test as ut
#
#_LOG = logging.getLogger(__name__)
#
#import dev_scripts.testing.run_tests as run_tests
#
#
#class Test_RunTests1(ut.TestCase):
#    def_test(self):
#
#    def test_build_pytest_opts1(self) -> None:
#        args = Dict()
#        args.test = "fast"
#        collect_opts, opts = run_tests._build_pytest_opts(args)
#
#    def test_build_pytest_opts2(self) -> None:
#        args = Dict()
#        args.test = "slow"
#        collect_opts, opts = run_tests._build_pytest_opts(args)
