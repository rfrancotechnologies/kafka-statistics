FROM microsoft/dotnet:2.2-runtime
WORKDIR /app
COPY ./bin/Release/netcoreapp2.1/publish /app
ENTRYPOINT ["dotnet", "/app/example-producer-prometheus-statistic-handler.dll"]
