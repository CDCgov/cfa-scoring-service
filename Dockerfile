FROM ghcr.io/rocker-org/tidyverse:4.4.3

RUN install2.r scoringutils optparse

COPY score.R .
