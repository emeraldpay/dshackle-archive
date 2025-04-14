use tokio::sync::mpsc::Receiver;

///
/// Produces a sorted stream from two **sorted** streams.
///
pub fn merge_sort<T>(mut left: Receiver<T>, mut right: Receiver<T>) -> Receiver<T> where T: PartialOrd + Send + Sync + 'static {
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tokio::spawn(async move {
        let mut left_val = left.recv().await;
        let mut right_val = right.recv().await;

        loop {
            match (&left_val, &right_val) {
                (Some(l), Some(r)) => {
                    if l <= r {
                        if let Err(_) = tx.send(left_val.take().unwrap()).await {
                            break;
                        }
                        left_val = left.recv().await;
                    } else {
                        if let Err(_) = tx.send(right_val.take().unwrap()).await {
                            break;
                        }
                        right_val = right.recv().await;
                    }
                }
                (Some(_), None) => {
                    if let Err(_) = tx.send(left_val.take().unwrap()).await {
                        break;
                    }
                    left_val = left.recv().await;
                }
                (None, Some(_)) => {
                    if let Err(_) = tx.send(right_val.take().unwrap()).await {
                        break;
                    }
                    right_val = right.recv().await;
                }
                (None, None) => break,
            }
        }
    });
    rx
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_sorted() {
        let left_data =  vec![1, 2,       5, 7,    9, 9,     11, 12,         15,         24,     30, 45];
        let right_data = vec![   2, 3, 4,    7, 8,       10, 11,     13, 14,     17, 21,     25];
        let (left_tx, left_rx) = tokio::sync::mpsc::channel(3);
        let (right_tx, right_rx) = tokio::sync::mpsc::channel(3);
        let mut result = super::merge_sort(left_rx, right_rx);
        tokio::spawn(async move {
            for i in left_data {
                left_tx.send(i).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis((i * 7) % 11)).await;
            }
        });
        tokio::spawn(async move {
            for i in right_data {
                right_tx.send(i).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis((i * 11) % 13)).await;
            }
        });

        let mut result_data = Vec::new();
        while let Some(data) = result.recv().await {
            result_data.push(data);
        }

        assert_eq!(result_data, vec![1, 2, 2, 3, 4, 5, 7, 7, 8, 9, 9, 10, 11, 11, 12, 13, 14, 15, 17, 21, 24, 25, 30, 45]);
    }
}
