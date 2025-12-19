from rest_framework import serializers

class MovieSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    title = serializers.CharField()
    popularity = serializers.FloatField()
    release_date = serializers.DateField()
    vote_count = serializers.IntegerField()