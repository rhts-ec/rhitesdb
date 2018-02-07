# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('cannula', '0008_auto_20171119_1115'),
    ]

    operations = [
        migrations.CreateModel(
            name='ValidationRule',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False, verbose_name='ID', auto_created=True)),
                ('name', models.CharField(max_length=128, unique=True)),
                ('left_expr', models.CharField(max_length=256)),
                ('right_expr', models.CharField(max_length=256)),
                ('operator', models.CharField(max_length=2)),
                ('data_elements', models.ManyToManyField(to='cannula.DataElement')),
            ],
        ),
    ]
