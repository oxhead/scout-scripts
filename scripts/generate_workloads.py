hibench_workloads = ['bayes.spark', 'dfsioe.hadoop', 'wordcount.spark', 'lr.spark', 'als.spark', 'aggregation.spark', 'aggregation.hadoop', 'pagerank.spark', 'join.spark', 'join.hadoop', 'pagerank.hadoop', 'scan.spark', 'scan.hadoop', 'sort.hadoop', 'sort.spark', 'terasort.hadoop', 'terasort.spark']

sparkperf_workloads = ['regression', 'als', 'fp-growth', 'block-matrix-mult', 'classification', 'chi-sq-feature', 'pic', 'gradient-boosted-tree', 'gmm', 'decision-tree', 'random-forest', 'kmeans', 'lda', 'chi-sq-gof', 'chi-sq-mat', 'naive-bayes', 'pca', 'summary-statistics', 'svd', 'pearson', 'word2vec', 'spearman']

workload_list = []
datasize = 'small'

for w in hibench_workloads:
    app, framework = w.split('.')
    workload_list.append('hibench {} {} {} 1'.format(framework, app, datasize))

for app in sparkperf_workloads:
    workload_list.append('sparkperf spark1.5 {} {} 1'.format(app, datasize))

workload_strs = ""
for w in workload_list:
    print(w)
    workload_strs += ' -w "' + w + '"'

print("myaws run {} --instance-type c4.large --instance-num 3 --ami ami-ca33a7b5 --keyname osr --cluster-mode n+1 --s3-bucket scout-dataset-test --no-terminate --no-dry-run".format(workload_strs))
