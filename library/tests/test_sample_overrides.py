import pathlib

import doppeldata as sd
import pytest

pl = pytest.importorskip('polars')

FIXTURE = pathlib.Path(__file__).parents[2] / 'crates' / 'core' / 'tests' / 'every-column-kind.profile.yaml'


def test_pinning_a_conditional_parent_draws_its_child_from_the_pinned_value_and_leaves_every_other_column():
    profile = sd.Profile.load(FIXTURE)
    drawn = pl.DataFrame(sd.generate(profile, rows=2000, seed=1))
    assert pl.DataFrame(sd.sample(profile, rows=2000, seed=1)).equals(drawn)
    pinned = pl.DataFrame(sd.sample(profile, rows=2000, seed=1, overrides={'channel': 'store'}))
    assert pinned['channel'].unique().to_list() == ['store']
    # paid_amount's level comes from its own stream and its curve from channel, so a row drawn as store keeps its amount.
    already = drawn['channel'] == 'store'
    assert pinned.filter(already)['paid_amount'].equals(drawn.filter(already)['paid_amount'])
    assert pinned['paid_amount'].median() < drawn['paid_amount'].median()
    unread = [name for name in drawn.columns if name not in ('channel', 'paid_amount')]
    assert pinned.select(unread).equals(drawn.select(unread))
