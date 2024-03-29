import os

from tasks.eva_3090.archive_to_lts import archive_directory


def test_archive_directory():
    resources = os.path.join(os.path.dirname(__file__), 'resources')
    src_dir = os.path.join(resources, 'src')
    dest_dir = os.path.join(resources, 'dest')
    scratch_dir = os.path.join(resources, 'scratch')

    archive_directory(src_dir, scratch_dir, dest_dir, filter_patterns=['do_not_want'])
