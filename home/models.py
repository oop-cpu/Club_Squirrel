from django.db import models

class userSettings(models.Model):
    playerName = models.CharField(max_length=50)
    musicValue = models.PositiveIntegerField(default=50)
    soundValue = models.PositiveIntegerField(default=50)
